package ae

import (
	"container/list"
	"log"
	"syscall"
	"time"

	"github.com/IfanTsai/metis/socket"
	"github.com/pkg/errors"
)

const (
	defaultNearestDeltaMs = 1000
	defaultEpollWaitMs    = 10
)

type (
	TypeFileEvent int
	TypeTimeEvent int

	FileProc func(el *EventLoop, fd socket.FD, clientData any)
	TimeProc func(el *EventLoop, id int64, clientData any)
)

const (
	TypeFileEventReadable TypeFileEvent = 0x1 << iota
	TypeFileEventWritable
)

const (
	TypeTimeEventNormal TypeTimeEvent = 0x1 << iota
	TypeTimeEventOnce
)

type FileEvent struct {
	fd         socket.FD
	mask       TypeFileEvent
	proc       FileProc
	clientData any
}

type TimeEvent struct {
	id         int64
	mask       TypeTimeEvent
	when       int64
	interval   int64
	proc       TimeProc
	clientData any
}

type EventLoop struct {
	FileEventMap    map[int]*FileEvent
	TimeEventHead   *list.List // element is *TimeEvent
	TimeEventNextID int64
	fileEventFd     int
	stop            bool
}

// NewEventLoop creates a new event loop
func NewEventLoop() (*EventLoop, error) {
	epFd, err := syscall.EpollCreate1(0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create epoll")
	}

	return &EventLoop{
		FileEventMap:  make(map[int]*FileEvent),
		TimeEventHead: list.New(),
		fileEventFd:   epFd,
		stop:          false,
	}, nil
}

// AddFileEvent adds file event to event loop
func (el *EventLoop) AddFileEvent(fd socket.FD, mask TypeFileEvent, proc FileProc, clientData any) error {
	fileEventMapKey := getFileEventMapKey(fd, mask)
	if _, ok := el.FileEventMap[fileEventMapKey]; ok {
		return errors.New("file event already exists")
	}

	op := syscall.EPOLL_CTL_ADD
	ev := el.getEpollEventMask(fd)
	if ev != 0 {
		op = syscall.EPOLL_CTL_MOD
	}

	ev |= getEpollEventType(mask)
	if err := syscall.EpollCtl(el.fileEventFd, op, int(fd), &syscall.EpollEvent{Events: ev, Fd: int32(fd)}); err != nil {
		return errors.Wrap(err, "epoll ctl failed")
	}

	el.FileEventMap[fileEventMapKey] = &FileEvent{
		fd:         fd,
		mask:       mask,
		proc:       proc,
		clientData: clientData,
	}

	return nil
}

// RemoveFileEvent removes file event from event loop
func (el *EventLoop) RemoveFileEvent(fd socket.FD, mask TypeFileEvent) error {
	fileEventMapKey := getFileEventMapKey(fd, mask)
	if _, ok := el.FileEventMap[fileEventMapKey]; !ok {
		return errors.New("file event not exists")
	}

	op := syscall.EPOLL_CTL_DEL
	ev := el.getEpollEventMask(fd)
	ev &= ^getEpollEventType(mask) // clear mask which type will be removed
	if ev != 0 {
		op = syscall.EPOLL_CTL_MOD
	}

	if err := syscall.EpollCtl(el.fileEventFd, op, int(fd), &syscall.EpollEvent{Events: ev, Fd: int32(fd)}); err != nil {
		return errors.Wrap(err, "epoll ctl failed")
	}

	delete(el.FileEventMap, fileEventMapKey)

	return nil
}

// AddTimeEvent adds time event to event loop
func (el *EventLoop) AddTimeEvent(mask TypeTimeEvent, interval int64, proc TimeProc, clientData any) error {
	el.TimeEventHead.PushFront(&TimeEvent{
		id:         el.TimeEventNextID,
		mask:       mask,
		when:       now() + interval,
		interval:   interval,
		proc:       proc,
		clientData: clientData,
	})

	el.TimeEventNextID++

	return nil
}

// RemoveTimeEvent removes time event from event loop
func (el *EventLoop) RemoveTimeEvent(id int64) error {
	for e := el.TimeEventHead.Front(); e != nil; e = e.Next() {
		te := e.Value.(*TimeEvent)
		if te.id == id {
			el.TimeEventHead.Remove(e)

			return nil
		}
	}

	return errors.New("time event not exists")
}

// Main runs event loop
func (el *EventLoop) Main() error {
	for !el.stop {
		timeEvents, fileEvents, err := el.wait()
		if err != nil && !errors.Is(err, syscall.EINTR) {
			log.Println("wait failed:", err)
		}

		if err := el.processEvents(timeEvents, fileEvents); err != nil {
			log.Println("process events failed:", err)
		}
	}

	return socket.FD(el.fileEventFd).Close()
}

// Stop stops event loop
func (el *EventLoop) Stop() {
	el.stop = true
}

// processEvents processes time events and file events
func (el *EventLoop) processEvents(timeEvents []*TimeEvent, fileEvents []*FileEvent) error {
	for _, te := range timeEvents {
		te.proc(el, te.id, te.clientData)

		if te.mask == TypeTimeEventOnce {
			if err := el.RemoveTimeEvent(te.id); err != nil {
				return errors.Wrapf(err, "failed to remove time event: %v", te)
			}
		} else {
			te.when = now() + te.interval
		}
	}

	for _, fe := range fileEvents {
		fe.proc(el, fe.fd, fe.clientData)
	}

	return nil
}

// wait waits for nearest time of time events until any event is ready
func (el *EventLoop) wait() ([]*TimeEvent, []*FileEvent, error) {
	timeout := el.getNearestTime(defaultNearestDeltaMs) - now()
	if timeout <= 0 {
		timeout = defaultEpollWaitMs
	}

	events := make([]syscall.EpollEvent, 1024)
	nReady, err := syscall.EpollWait(el.fileEventFd, events, int(timeout))
	if err != nil {
		return nil, nil, errors.Wrap(err, "epoll wait failed")
	}

	var (
		timeEvents []*TimeEvent
		fileEvents []*FileEvent
	)

	epollEventTypes := []uint32{syscall.EPOLLIN, syscall.EPOLLOUT}
	for i := 0; i < nReady; i++ {
		ev := events[i]
		for _, epollEventType := range epollEventTypes {
			if ev.Events&epollEventType != 0 {
				fileEvent, ok := el.FileEventMap[getFileEventMapKey(socket.FD(ev.Fd), getFileEventType(epollEventType))]
				if ok {
					fileEvents = append(fileEvents, fileEvent)
				}
			}
		}
	}

	nowMs := now()
	for e := el.TimeEventHead.Front(); e != nil; e = e.Next() {
		te := e.Value.(*TimeEvent)
		if te.when <= nowMs {
			timeEvents = append(timeEvents, te)
		}
	}

	return timeEvents, fileEvents, nil
}

func (el *EventLoop) getNearestTime(defaultNearestDelta int64) int64 {
	nearest := now() + defaultNearestDelta
	for e := el.TimeEventHead.Front(); e != nil; e = e.Next() {
		te := e.Value.(*TimeEvent)
		if te.when < nearest {
			nearest = te.when
		}
	}

	return nearest
}

// getEpollEventMask returns current epoll event mask
func (el *EventLoop) getEpollEventMask(fd socket.FD) uint32 {
	var mask uint32

	if _, ok := el.FileEventMap[getFileEventMapKey(fd, TypeFileEventReadable)]; ok {
		mask |= syscall.EPOLLIN
	}

	if _, ok := el.FileEventMap[getFileEventMapKey(fd, TypeFileEventWritable)]; ok {
		mask |= syscall.EPOLLOUT
	}

	return mask
}

// getFileEventMapKey returns file event map key
func getFileEventMapKey(fd socket.FD, mask TypeFileEvent) int {
	return int(fd)<<16 | int(mask)
}

// getEpollEventType returns epoll event type by file event type
func getEpollEventType(fileEventType TypeFileEvent) uint32 {
	switch fileEventType {
	case TypeFileEventReadable:
		return syscall.EPOLLIN
	case TypeFileEventWritable:
		return syscall.EPOLLOUT
	default:
		return 0
	}
}

func getFileEventType(epollEventType uint32) TypeFileEvent {
	switch epollEventType {
	case syscall.EPOLLIN:
		return TypeFileEventReadable
	case syscall.EPOLLOUT:
		return TypeFileEventWritable
	default:
		return 0
	}
}

// now returns current time in milliseconds
func now() int64 {
	return time.Now().UnixMilli()
}
