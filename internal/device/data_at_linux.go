//go:build linux

package device

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Host-side timing for the AT data backend. The first lease budget follows
// the vendor FAQ (DHCP completes within tens of seconds after the PDP is up);
// stop must be quick so disable stays fail-closed.
const (
	atHostFirstLeaseTimeout = 60 * time.Second
	atHostConfigureTimeout  = 15 * time.Second
	atHostStopTimeout       = 10 * time.Second

	// atHostEventPollInterval is how often the non-blocking FIFO reader wakes
	// to re-check the session context while the DHCP hook is idle. It only
	// bounds shutdown latency, not event delivery: a hook write wakes the
	// next read immediately.
	atHostEventPollInterval = 50 * time.Millisecond
)

func newATHostDataLink() atHostDataLink {
	return &linuxATHostDataLink{}
}

type linuxATHostDataLink struct{}

// Start brings the cellular interface up, runs a foreground busybox udhcpc
// whose hook script reports lease events over a FIFO, and returns once the
// first lease is applied. The session owns the DHCP process and the protected
// policy-routing generation until Close.
func (link *linuxATHostDataLink) Start(ctx context.Context, networkInterface string) (atHostDataSession, error) {
	networkInterface = strings.TrimSpace(networkInterface)
	if networkInterface == "" {
		return nil, fmt.Errorf("%w: a network interface is required", ErrDataBackendUnavailable)
	}
	ipCommand, err := lookPathIP("ip")
	if err != nil {
		return nil, fmt.Errorf("%w: install iproute2 to control %s", ErrDataBackendUnavailable, networkInterface)
	}
	busybox, err := exec.LookPath("busybox")
	if err != nil {
		return nil, fmt.Errorf("%w: busybox udhcpc is required for AT host data on %s", ErrDataBackendUnavailable, networkInterface)
	}

	dir, err := os.MkdirTemp("", "vocat-atdata-")
	if err != nil {
		return nil, err
	}
	eventPath := filepath.Join(dir, "events")
	if err := syscall.Mkfifo(eventPath, 0o600); err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("create DHCP event channel: %w", err)
	}
	scriptPath := filepath.Join(dir, "hook.sh")
	scriptText := fmt.Sprintf(`#!/bin/sh
case "$1" in
  bound|renew)
    (umask 077; printf 'event=%%s\nip=%%s\nsubnet=%%s\nrouter=%%s\ndns=%%s\n\n' "$1" "$ip" "$subnet" "$router" "$dns" > %q)
    ;;
  deconfig|released|carrierlost|fail)
    (umask 077; printf 'event=deconfig\n\n' > %q)
    ;;
esac
exit 0
`, eventPath, eventPath)
	if err := os.WriteFile(scriptPath, []byte(scriptText), 0o700); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &linuxATHostDataSession{
		networkInterface: networkInterface,
		ipCommand:        ipCommand,
		runner:           hostCommandOutput,
		dir:              dir,
		scriptPath:       scriptPath,
		eventPath:        eventPath,
		background:       sessionCtx,
		cancel:           cancel,
		firstLease:       make(chan struct{}),
		done:             make(chan struct{}),
	}

	// A daemon or modem restart can leave the previous generation on the
	// interface; the lease must always start from a clean host state.
	session.cleanupHost(ctx)
	if result, linkErr := hostCommandOutput(ctx, ipCommand, "link", "set", "dev", networkInterface, "up"); linkErr != nil {
		_ = session.Close(ctx)
		return nil, fmt.Errorf("set %s up: %w: %s", networkInterface, linkErr, strings.TrimSpace(string(result)))
	}

	// The FIFO reader must exist before udhcpc starts writing events.
	session.wg.Add(1)
	go session.readEvents()

	process, err := startForegroundUDHCPC(sessionCtx, busybox, scriptPath, dir, networkInterface)
	if err != nil {
		_ = session.Close(ctx)
		return nil, err
	}
	session.process = process
	session.wg.Add(1)
	go session.watchProcess()

	if err := session.awaitFirstLease(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

// awaitFirstLease waits for the first DHCP lease within the lease timeout and
// closes the session on every failure path.
//
// udhcpc without -n retries discovery indefinitely, so a carrier that never
// answers DHCP would block Start forever while the caller holds the device
// operation lock. The bound therefore lives here rather than in the
// caller's context, which may carry no deadline at all.
func (session *linuxATHostDataSession) awaitFirstLease(ctx context.Context) error {
	leaseContext, cancelLease := context.WithTimeout(ctx, atHostFirstLeaseTimeout)
	defer cancelLease()
	if err := session.waitFirstLease(leaseContext); err != nil {
		// Close on every failure path, not only the cancellation branch: a
		// lost lease or an exited DHCP process must not leave the process
		// and its temp directory behind.
		_ = session.Close(context.Background())
		return err
	}
	return nil
}

type atHostProcess interface {
	Wait() error
	Stop(ctx context.Context) error
}

type execHostProcess struct {
	cmd     *exec.Cmd
	done    chan struct{}
	waitErr error
	once    sync.Once
}

func newExecHostProcess(cmd *exec.Cmd) *execHostProcess {
	process := &execHostProcess{cmd: cmd, done: make(chan struct{})}
	go func() {
		process.waitErr = cmd.Wait()
		close(process.done)
	}()
	return process
}

func (process *execHostProcess) Wait() error {
	<-process.done
	return process.waitErr
}

func (process *execHostProcess) Stop(ctx context.Context) error {
	if process.cmd.Process != nil {
		_ = process.cmd.Process.Signal(syscall.SIGTERM)
	}
	select {
	case <-process.done:
		return nil
	case <-ctx.Done():
		if process.cmd.Process != nil {
			_ = process.cmd.Process.Kill()
		}
		<-process.done
		return ctx.Err()
	}
}

func startForegroundUDHCPC(
	ctx context.Context,
	busybox, scriptPath, dir, networkInterface string,
) (atHostProcess, error) {
	command := exec.CommandContext(ctx, busybox, "udhcpc", "-f", "-i", networkInterface, "-s", scriptPath)
	logFile, err := os.Create(filepath.Join(dir, "udhcpc.log"))
	if err == nil {
		command.Stdout = logFile
		command.Stderr = logFile
		// exec.Cmd.Wait does not close a file the caller supplied, and the
		// child keeps its own copy of the descriptor. Closing it here keeps
		// a long-running daemon from collecting one descriptor per session.
		defer func() { _ = logFile.Close() }()
	}
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("start udhcpc on %s: %w", networkInterface, err)
	}
	return newExecHostProcess(command), nil
}

type atDHCPLease struct {
	Address string
	Prefix  int
	Gateway string
	DNS     []string
}

type linuxATHostDataSession struct {
	networkInterface string
	ipCommand        string
	runner           func(context.Context, string, ...string) ([]byte, error)
	process          atHostProcess
	dir              string
	scriptPath       string
	eventPath        string
	background       context.Context
	cancel           context.CancelFunc

	// eventMu serializes lease-event application against the process-exit
	// cleanup. Without it, a delayed bound event could finish after
	// watchProcess tore the routes and lease down, resurrecting a session
	// whose DHCP process has already exited.
	eventMu       sync.Mutex
	eventsStopped bool

	mu         sync.Mutex
	lease      *atDHCPLease
	lastErr    error
	closed     bool
	firstLease chan struct{}
	firstOnce  sync.Once
	done       chan struct{}
	wg         sync.WaitGroup
}

// Status reports whether the protected host lease is currently established.
func (session *linuxATHostDataSession) Status(context.Context) (bool, string, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return false, "AT host data session is closed", nil
	}
	if session.lease == nil {
		if session.lastErr != nil {
			return false, "AT host data lease is not established", session.lastErr
		}
		return false, "AT host data lease is not established", nil
	}
	return true, fmt.Sprintf(
		"host lease %s/%d on %s",
		session.lease.Address, session.lease.Prefix, session.networkInterface,
	), nil
}

// Close stops the DHCP process, waits for it to exit, and removes every host
// path this session created (protected routes, addresses, DNS file).
func (session *linuxATHostDataSession) Close(ctx context.Context) error {
	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return nil
	}
	session.closed = true
	session.mu.Unlock()

	// Stop accepting lease events before tearing anything down, so an event
	// that is already buffered cannot reinstall the routes Close removes.
	session.stopLeaseEvents()
	if session.cancel != nil {
		session.cancel()
	}
	if session.process != nil {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), atHostStopTimeout)
		_ = session.process.Stop(stopCtx)
		cancelStop()
	}
	// The reader opens the FIFO read-write and non-blocking, so it is never
	// parked in open(2); the cancelled context ends its poll loop.
	session.wg.Wait()

	cleanupContext := ctx
	var cancelCleanup context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		cleanupContext, cancelCleanup = context.WithTimeout(context.Background(), atHostStopTimeout)
		defer cancelCleanup()
	}
	session.cleanupHost(cleanupContext)
	if session.dir != "" {
		_ = os.RemoveAll(session.dir)
	}
	return nil
}

// cleanupHost removes the protected routing generation and the interface
// address. It never touches the main routing table.
func (session *linuxATHostDataSession) cleanupHost(ctx context.Context) {
	clearExportProxyRoute(ctx, session.networkInterface)
	if session.runner == nil || session.ipCommand == "" {
		return
	}
	_, _ = session.runner(ctx, session.ipCommand, "-4", "addr", "flush", "dev", session.networkInterface, "scope", "global")
	_, _ = session.runner(ctx, session.ipCommand, "link", "set", "dev", session.networkInterface, "down")
}

// readEvents drains the DHCP hook records from the event FIFO.
//
// The FIFO is opened read-write and non-blocking so shutdown can never park in
// open(2): a reader that lost the race against the DHCP writer used to block
// forever, and Close then hung in wg.Wait() while holding the device
// operation lock. Read-write keeps the FIFO from ever reporting EOF, and the
// EAGAIN poll loop observes the session context between attempts.
func (session *linuxATHostDataSession) readEvents() {
	defer session.wg.Done()
	background := session.background
	if background == nil {
		background = context.Background()
	}
	fd, err := syscall.Open(session.eventPath, syscall.O_RDWR|syscall.O_NONBLOCK, 0)
	if err != nil {
		return
	}
	defer func() { _ = syscall.Close(fd) }()

	buffer := make([]byte, 4096)
	var pending []byte
	values := make(map[string]string)
	for {
		if background.Err() != nil {
			return
		}
		read, readErr := syscall.Read(fd, buffer)
		if read > 0 {
			pending = append(pending, buffer[:read]...)
			for {
				index := bytes.IndexByte(pending, '\n')
				if index < 0 {
					break
				}
				line := strings.TrimSpace(string(pending[:index]))
				pending = pending[index+1:]
				if line == "" {
					// A blank line terminates a hook record.
					if len(values) > 0 {
						session.handleLeaseEvent(values)
						values = make(map[string]string)
					}
					continue
				}
				if key, value, found := strings.Cut(line, "="); found {
					values[strings.TrimSpace(key)] = strings.TrimSpace(value)
				}
			}
			continue
		}
		if readErr != nil && readErr != syscall.EAGAIN && readErr != syscall.EWOULDBLOCK {
			return
		}
		if !session.waitForEventPoll(background) {
			return
		}
	}
}

// waitForEventPoll sleeps for one poll interval, returning false as soon as
// the session context is cancelled so the reader can exit.
func (session *linuxATHostDataSession) waitForEventPoll(background context.Context) bool {
	timer := time.NewTimer(atHostEventPollInterval)
	defer timer.Stop()
	select {
	case <-background.Done():
		return false
	case <-timer.C:
		return true
	}
}

// handleLeaseEvent applies a hook record unless the lease events have been
// stopped. It is the only path from event parsing to the host state, so the
// stop flag and event application are atomic with respect to each other.
func (session *linuxATHostDataSession) handleLeaseEvent(values map[string]string) {
	session.eventMu.Lock()
	defer session.eventMu.Unlock()
	if session.eventsStopped {
		return
	}
	session.applyEvent(values)
}

// stopLeaseEvents permanently disables lease-event application. It is called
// by Close and by the DHCP process watcher before the process-exit cleanup, so
// a delayed bound/renew event can never rebuild the routes that cleanup
// removed.
func (session *linuxATHostDataSession) stopLeaseEvents() {
	session.eventMu.Lock()
	session.eventsStopped = true
	session.eventMu.Unlock()
}

func (session *linuxATHostDataSession) watchProcess() {
	defer session.wg.Done()
	_ = session.process.Wait()
	// An unexpected DHCP exit (interface down, carrier loss) drops the lease
	// and the protected routes; Status then reports disconnected so the
	// monitor can recover. Stop lease events first and hold eventMu for the
	// cleanup, so a bound event racing the exit cannot resurrect the session.
	session.stopLeaseEvents()
	session.eventMu.Lock()
	session.applyEvent(map[string]string{"event": "deconfig"})
	session.eventMu.Unlock()
	close(session.done)
}

// handleEventLines parses hook-script records (key=value lines separated by a
// blank line) and applies them in order.
func (session *linuxATHostDataSession) handleEventLines(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	values := make(map[string]string)
	flush := func() {
		if len(values) == 0 {
			return
		}
		session.handleLeaseEvent(values)
		values = make(map[string]string)
	}
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			flush()
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if found {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	flush()
}

func (session *linuxATHostDataSession) applyEvent(values map[string]string) {
	base := session.background
	if base == nil {
		base = context.Background()
	}
	switch strings.ToLower(values["event"]) {
	case "bound", "renew":
		address := net.ParseIP(values["ip"]).To4()
		maskIP := net.ParseIP(values["subnet"]).To4()
		if address == nil || maskIP == nil {
			session.setLeaseError(errors.New("DHCP returned no valid IPv4 address/subnet"))
			return
		}
		ones, bits := net.IPMask(maskIP).Size()
		if bits != 32 || ones < 0 {
			session.setLeaseError(errors.New("DHCP returned an invalid IPv4 subnet"))
			return
		}
		gateway := ""
		if routers := strings.Fields(values["router"]); len(routers) > 0 {
			if net.ParseIP(routers[0]).To4() == nil {
				session.setLeaseError(errors.New("DHCP returned an invalid IPv4 gateway"))
				return
			}
			gateway = routers[0]
		}
		dns := strings.Fields(values["dns"])
		configureContext, cancel := context.WithTimeout(base, atHostConfigureTimeout)
		detail, err := configureExportProxyIPv4(
			configureContext, session.ipCommand, session.networkInterface,
			address.String(), ones, gateway, dns,
		)
		cancel()
		if err != nil {
			// The failed configure already tore down the protected routes
			// and rule, so the previous lease no longer describes the host
			// path. Keeping it would let Status report "connected" from a
			// stale lease and the monitor would never recover the session.
			session.mu.Lock()
			session.lease = nil
			session.lastErr = err
			session.mu.Unlock()
			return
		}
		session.mu.Lock()
		session.lease = &atDHCPLease{
			Address: address.String(),
			Prefix:  ones,
			Gateway: gateway,
			DNS:     dns,
		}
		session.lastErr = nil
		session.mu.Unlock()
		session.firstOnce.Do(func() { close(session.firstLease) })
		_ = detail
	case "deconfig":
		cleanupContext, cancel := context.WithTimeout(base, atHostConfigureTimeout)
		clearExportProxyRoute(cleanupContext, session.networkInterface)
		if session.runner != nil && session.ipCommand != "" {
			_, _ = session.runner(cleanupContext, session.ipCommand, "-4", "addr", "flush", "dev", session.networkInterface, "scope", "global")
		}
		cancel()
		session.mu.Lock()
		session.lease = nil
		session.mu.Unlock()
	}
}

func (session *linuxATHostDataSession) setLeaseError(err error) {
	session.mu.Lock()
	session.lastErr = err
	session.mu.Unlock()
}

func (session *linuxATHostDataSession) waitFirstLease(ctx context.Context) error {
	select {
	case <-session.firstLease:
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.lease != nil {
			return nil
		}
		if session.lastErr != nil {
			return fmt.Errorf("apply first AT host data lease: %w", session.lastErr)
		}
		return errors.New("AT host data session lost its first lease")
	case <-session.done:
		return errors.New("AT host data DHCP process exited before providing a lease")
	case <-ctx.Done():
		_ = session.Close(context.Background())
		return fmt.Errorf("wait for AT host data lease on %s: %w", session.networkInterface, ctx.Err())
	}
}
