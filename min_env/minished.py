import time
import heapq
from collections import namedtuple
try:
    import threading
except ImportError:
    import dummy_threading as threading
try:
    from time import monotonic as _time
except ImportError:
    from time import time as _time

__all__ = ["scheduler"]


class Event(namedtuple('Event', 'time, priority, action, argument, kwargs')):
    def __eq__(s, o): return (s.time, s.priority) == (o.time, o.priority)
    def __ne__(s, o): return (s.time, s.priority) != (o.time, o.priority)
    def __lt__(s, o): return (s.time, s.priority) <  (o.time, o.priority)
    def __le__(s, o): return (s.time, s.priority) <= (o.time, o.priority)
    def __gt__(s, o): return (s.time, s.priority) >  (o.time, o.priority)
    def __ge__(s, o): return (s.time, s.priority) >= (o.time, o.priority)


_sentinel = object()


class scheduler:

    def __init__(self, timefunc=_time, delayfunc=time.sleep):
        """Initialize a new instance, passing the time and delay
        functions"""
        self._queue = []
        self._lock = threading.RLock()
        self.timefunc = timefunc
        self.delayfunc = delayfunc

    def enterabs(self, time, priority, action, argument=(), kwargs=_sentinel):
        """Enter a new event in the queue at an absolute time.
        Returns an ID for the event which can be used to remove it,
        if necessary.
        """
        if kwargs is _sentinel:
            kwargs = {}
        event = Event(time, priority, action, argument, kwargs)
        with self._lock:
            heapq.heappush(self._queue, event)
        return event  # The ID

    def enter(self, delay, priority, action, argument=(), kwargs=_sentinel):
        """A variant that specifies the time as a relative time.
        This is actually the more commonly used interface.
        """
        time = self.timefunc() + delay
        return self.enterabs(time, priority, action, argument, kwargs)

    def cancel(self, event):
        """Remove an event from the queue.
        This must be presented the ID as returned by enter().
        If the event is not in the queue, this raises ValueError.
        """
        with self._lock:
            self._queue.remove(event)
            heapq.heapify(self._queue)

    def empty(self):
        """Check whether the queue is empty."""
        with self._lock:
            return not self._queue

    def run(self, blocking=True):
        """Execute events until the queue is empty.
        If blocking is False executes the scheduled events due to
        expire soonest (if any) and then return the deadline of the
        next scheduled call in the scheduler.
        When there is a positive delay until the first event, the
        delay function is called and the event is left in the queue;
        otherwise, the event is removed from the queue and executed
        (its action function is called, passing it the argument).  If
        the delay function returns prematurely, it is simply
        restarted.
        It is legal for both the delay function and the action
        function to modify the queue or to raise an exception;
        exceptions are not caught but the scheduler's state remains
        well-defined so run() may be called again.
        A questionable hack is added to allow other threads to run:
        just after an event is executed, a delay of 0 is executed, to
        avoid monopolizing the CPU when other threads are also
        runnable.
        """
        # localize variable access to minimize overhead
        # and to improve thread safety
        lock = self._lock
        q = self._queue
        delayfunc = self.delayfunc
        timefunc = self.timefunc
        pop = heapq.heappop
        while True:
            with lock:
                if not q:
                    break
                time, priority, action, argument, kwargs = q[0]
                now = timefunc()
                if time > now:
                    delay = True
                else:
                    delay = False
                    pop(q)
            if delay:
                if not blocking:
                    return time - now
                delayfunc(time - now)
            else:
                action(*argument, **kwargs)
                delayfunc(0)   # Let other threads run

    @property
    def queue(self):
        """An ordered list of upcoming events.
        Events are named tuples with fields for:
            time, priority, action, arguments, kwargs
        """
        # Use heapq to sort the queue rather than using 'sorted(self._queue)'.
        # With heapq, two events scheduled at the same time will show in
        # the actual order they would be retrieved.
        with self._lock:
            events = self._queue[:]
        return list(map(heapq.heappop, [events]*len(events)))
