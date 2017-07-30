
__version__ = (1, 1, '2017-07-27')


import sys, os, time, socket, select, types
import threading, queue
import itertools, collections, functools, traceback, platform, io, hashlib
from types import GeneratorType
from heapq import heappush, heappop
from urllib.request import unquote

if sys.version_info[:2] <= (3, 4) and platform.system() == 'Windows':
    from . import socketpair


__all__ = [
    'EventLoop', 'Event', 'Timeout', 'UnhandledEvent', 'get_poll', 'WAIT_EVENT',
    'EV_READ', 'EV_WRITE', 'EV_TASK_RESULT', 'EV_THREAD_RESULT',
    'EV_TIMER', 'EV_MSG', 'EV_BROADCAST'
]


class UnhandledEvent(Exception):

    def __init__(self, event):
        self.orig_event = event

    def __getitem__(self, key):
        return self.orig_event[key]

    def __getattr__(self, key):
        return getattr(self.orig_event, key)


class Timeout(Exception):
    pass

class _Quit(Exception):
    pass

# event type constants
EV_READ = 'RD'
EV_WRITE = 'WR'
EV_TASK_RESULT = 'TSKR'
EV_THREAD_RESULT = 'THRR'
EV_TIMER = 'TM'
EV_MSG = 'MSG'
EV_BROADCAST = 'BCST'


# request to event loop constants
WAIT_EVENT = 0
GET_POLL = 1

BEGIN_READ = -1
END_READ = -2

BEGIN_WRITE = -3
END_WRITE = -4

RUN_TASK = -5
RUN_TASK_FOR_RESULT = -6
RUN_THREAD_FOR_RESULT = -7

START_TIMER = -8
STOP_TIMER = -9

OPEN_MSGBOX = -10
CLOSE_MSGBOX = -11
SEND_MSG = -12
MSGBOX_FUNCTION = -13

LISTEN_BROADCAST = -14
IGNORE_BROADCAST = -15
SEND_BROADCAST = -16




# ---------------------------------------------------------
class Event:

    def __init__(self, type, **attrs):

        self.type = type
        self.__dict__.update(attrs)


    def __getitem__(self, key):
        return getattr(self, key)


    def is_timer(self, timer_id):
        return self.type == EV_TIMER and self.timer_id == timer_id

    def is_thread(self, thread_id):
        return self.type == EV_THREAD_RESULT and self.thread_id == thread_id

    def is_task(self, task):
        return self.type == EV_TASK_RESULT and self.task is task

    def is_read(self, obj):
        return self.type == EV_READ and self.obj is obj

    def is_write(self, obj):
         return self.type == EV_WRITE and self.obj is obj

    def is_msg(self, msgbox):
        return self.type == EV_MSG and self.msgbox == msgbox

    def is_broadcast(self, channel):
        return self.type == EV_BROADCAST and self.channel == channel



class Poll:

    def __init__(self, loop, task):

        self._loop = loop
        self._task = task

        self._read = []
        self._write = []
        self._timer = []
        self._msgbox = []
        self._thread = []
        self._send_result_to = None
        self._broadcast = []


    def wait_event(self):
        return WAIT_EVENT

    def begin_read(self, fd):

        if fd in self._loop._readers:
            if self._loop._readers[fd] is not self._task:
                raise ValueError('another task ({}) is reading on {}; current task is {}'.format(
                    describe_generator(self._loop._readers[fd]), fd, describe_generator(self._task))
                )

        elif fd in self._read:
            raise ValueError('already reading on {}'.format(fd))

        self._loop._readers[fd] = self._task
        self._read.append(fd)


    def end_read(self, fd):

        if fd in self._loop._readers:
            if self._loop._readers[fd] is not self._task:
                raise ValueError('another task ({}) is reading on {}; current task is {}'.format(
                    describe_generator(self._loop._readers[fd]), fd, describe_generator(self._task))
                )

        elif fd not in self._read:
            raise ValueError('not reading on {}'.format(fd))

        self._loop._readers.pop(fd)
        self._read.remove(fd)


    def begin_write(self, fd):

        if fd in self._loop._writers:
            if self._loop._writers[fd] is not self._task:
                raise ValueError('another task ({}) is reading on {}; current task is {}'.format(
                    describe_generator(self._loop._writers[fd]), fd, describe_generator(self._task))
                )

        elif fd in self._write:
            raise ValueError('already writing on {}'.format(fd))

        self._loop._writers[fd] = self._task
        self._write.append(fd)


    def end_write(self, fd):

        if fd in self._loop._writers:
            if self._loop._writers[fd] is not self._task:
                raise ValueError('another task ({}) is writing on {}; current task is {}'.format(
                    describe_generator(self._loop._writers[fd]), fd, describe_generator(self._task))
                )

        elif fd not in self._read:
            raise ValueError('not writing on {}'.format(fd))

        self._loop._writers.pop(fd)
        self._write.remove(fd)


    def start_timer(self, expire_delay):

        expire_time = expire_delay + time.time()
        timer_id = next(self._loop._id_generator)
        heappush(self._loop._timer_heap, (expire_time, timer_id))
        self._loop._timers[timer_id] = self._task
        self._timer.append(timer_id)

        return timer_id


    def stop_timer(self, timer_id):

        if timer_id in self._loop._timers:
            if self._task is self._loop._timers[timer_id]:
                self._loop._timers.pop(timer_id)
                self._timer.remove(timer_id)
            else:
                raise ValueError('not the owner of the timer id={}'.format(timer_id))
        else:
            throw = ValueError('no timer id {}'.format(timer_id))


    def run_task(self, task):
        self._loop.init_task(task)


    def run_task_for_result(self, task):

        self._loop.init_task(task, send_result_to=self._task)
        return task


    def open_msgbox(self, msgbox=None):

        if msgbox is None:
            msgbox = next(self._loop._id_generator)

        if msgbox in self._loop._msgboxes:
            raise ValueError('msgbox {!r} already taken'.format(msgbox))

        assert msgbox not in self._msgbox
        self._loop._msgboxes[msgbox] = self._task
        self._msgbox.append(msgbox)

        return msgbox


    def close_msgbox(self, msgbox):

        if msgbox not in self._loop._msgboxes:
            raise ValueError('no msgbox {!r}'.format(msgbox))
        elif self._loop._msgboxes[msgbox] is not self._task:
            raise ValueError('not the owner of msgbox {!r}'.format(msgbox))

        assert msgbox in self._msgbox
        self._loop._msgboxes.pop(msgbox)
        self._msgbox.remove(msgbox)


    def send_msg(self, msgbox, msg):

        if msgbox not in self._loop._msgboxes:
            raise ValueError('no msgbox named {!r}'.format(msgbox))
        else:
            self._loop._messages.append((msgbox, msg, None))


    def get_msgbox_send_function(self, msgbox):

        if msgbox not in self._msgbox:
            raise ValueError('not listening on msgbox {!r}'.format(msgbox))

        func = lambda item: self._loop._q2msgbox_queue.put_sync((msgbox, item))
        return func


    def listen_broadcast(self, channel):

        if channel not in self._broadcast:
            self._broadcast.append(channel)
            self._loop._broadcast_listeners[channel].add(self._task)
        else:
            raise ValueError('already listening on broadcast channel {!r}'.format(channel))


    def ignore_broadcast(self, channel):

        if channel not in self._broadcast:
            raise ValueError('non listening on broadcast channel {!r}'.format(channel))

        else:
            self._broadcast.remove(channel)
            self._loop._broadcast_listeners[channel].remove(self._task)
            if not self._loop._broadcast_listeners[channel]:
                self._loop._broadcast_listeners.pop(channel)


    def send_broadcast(self, channel, msg):

        listeners = self._loop._broadcast_listeners.get(channel, ())
        for dest_task in listeners:
            self._loop.append_late_event(
                dest_task,
                Event(EV_BROADCAST, channel=channel, msg=msg)
            )

        return len(listeners)


    def run_thread_for_result(self, target_func):
        return self._loop.start_thread(target_func, self._task)


class EventLoop:

    STOP_CHAR = b'\x00'

    # exit code constants
    NO_MORE_TASKS = 1
    INTERRUPTED = 2

    printlog = lambda self, *args: print(*args, file=sys.stderr)

    def __init__(self, *start_tasks):

        self._tasks = {}    # <task>: <poll object>

        self._id_generator = itertools.count()
        self._readers = {}
        self._writers = {}

        self._timer_heap = []
        self._timers = {}

        self._msgboxes = {}
        self._messages = collections.deque()
        self._broadcast_listeners = collections.defaultdict(set)

        self._threads = {}
        self._threads_by_id = {}

        # events to be sent by the end of each main loop
        self._late_events = collections.deque()

        # pair of sockets to be used to interrupt the select().
        # To be used by other threads.
        self._r_wakeup_sock, self._w_wakeup_sock = socket.socketpair()
        self._w_wakeup_sock.setblocking(True)   # this socket can be blocking. Whenever other threads
                                                # write on it, it's ok if they get blocked.

        self._readers[self._r_wakeup_sock] = None
        self._thread_result_queue = EventLoopQueue(self._w_wakeup_sock.send, b'T')
        self._q2msgbox_queue = EventLoopQueue(self._w_wakeup_sock.send, b'M')

        # tasks passed to the constructor will be started inside run()
        self.start_tasks = start_tasks


    def init_task(self, task, send_result_to=None):

        if task in self._tasks:
            raise ValueError('task {} already running'.format(task))
        elif not isinstance(task, GeneratorType):
            raise TypeError

        self._tasks[task] = Poll(self, task)
        if send_result_to:
            self._tasks[task]._send_result_to = send_result_to

        # It's important to run immediately the new task, in order to
        # let it to do some initializations (like opening msgboxes)
        # before its parent task resumes.
        self.resume_task(task, None)


    def run(self):

        for t in self.start_tasks:
            self.init_task(t)

        del self.start_tasks

        try:
            self._main_loop()
            return self.NO_MORE_TASKS

        except _Quit:
            return self.INTERRUPTED


    def _main_loop(self):

        default_timeout = 2 if platform.system() == 'Windows' else 10

        poll_map = self._tasks
        int_sock = self._r_wakeup_sock
        rlist = self._readers
        wlist = self._writers
        elist = []
        messages = self._messages
        timer_heap = self._timer_heap
        late_events = self._late_events
        read_time = time.time

        while poll_map:

            # calcolo timeout
            if late_events or messages:
                timeout = 0
            elif timer_heap:
                timeout = min(default_timeout, max(0, timer_heap[0][0] - read_time()))
            else:
                timeout = default_timeout

            r, w, e = select.select(rlist, wlist, elist, timeout)

            # handle writes
            while w:
                sock = w.pop(0)
                if sock in wlist:
                    self.resume_task(wlist[sock], Event(EV_WRITE, obj=sock))

            # handle reads
            while r:
                sock = r.pop(0)

                if sock is int_sock:

                    id_char = int_sock.recv(1)

                    if id_char == self._thread_result_queue.id_char:
                        result = self._thread_result_queue.get(False)   # an item MUST be present
                        assert 'confirm_queue' not in result
                        self.manage_thread_result(result['item'])

                    elif id_char == self._q2msgbox_queue.id_char:

                        result = self._q2msgbox_queue.get(False)
                        dest_msgbox, msg = result['item']
                        if dest_msgbox in self._msgboxes:
                            messages.append((dest_msgbox, msg, {'confirm_queue': result['confirm_queue']}))

                        else:
                            result['confirm_queue'].put(ValueError('no msgbox {}'.format(dest_msgbox)), block=False)

                    elif id_char == self.STOP_CHAR:
                        raise _Quit

                    else:
                        raise AssertionError(id_char)

                else:
                    if sock in rlist:
                        self.resume_task(rlist[sock], Event(EV_READ, obj=sock))

            sock = None

            # handle messages
            while messages:
                dest_msgbox, msg, other = messages.popleft()
                if dest_msgbox in self._msgboxes:
                    if other and 'confirm_queue' in other:
                        other['confirm_queue'].put(None, block=False)
                    self.resume_task(
                        self._msgboxes[dest_msgbox],
                        Event(EV_MSG, msgbox=dest_msgbox, msg=msg)
                    )
                else:
                    self.printlog('lost message for msgbox {}'.format(repr(dest_msgbox)))

            dest_msgbox = msg = None

            # handle timers
            while timer_heap and read_time() >= timer_heap[0][0]:

                expire_time, timer_id = heappop(timer_heap)

                if timer_id in self._timers:
                    task = self._timers.pop(timer_id)
                    poll_map[task]._timer.remove(timer_id)
                    self.resume_task(task, Event(EV_TIMER, timer_id=timer_id))
                else:
                    # this timer was cancelled
                    pass

            if late_events:
                # Set up a new late events queue so new events
                # enqueued here will be delivered at the next iteration
                self._late_events = collections.deque()

                while late_events:
                    task, event = late_events.popleft()
                    if task in poll_map:
                        assert event is not None
                        self.resume_task(task, event)
                    else:
                        self.printlog('late event lost', task, event)

                late_events = self._late_events


    def resume_task(self, task, event):

        task_poll = self._tasks[task]
        send = event
        throw = None

        try:
            while True:

                if throw:
                    #print('***THROW', throw, generator_position(task))
                    rs = task.throw(throw)
                else:
                    #print('****SEND', send, generator_position(task))
                    rs = task.send(send)

                if rs == WAIT_EVENT:
                    break

                elif rs == GET_POLL:
                    send = task_poll
                    throw = None

                else:
                    throw = ValueError('unexpected value {!r}'.format(rs))
                    send = None

        except StopIteration as err:
            self.cleanup_terminated_task(task)

            if task_poll._send_result_to:
                dest_task = task_poll._send_result_to
                if dest_task in self._tasks:
                    self.append_late_event(
                        dest_task,
                        Event(EV_TASK_RESULT, task=task, result=err.value, exception=None)
                    )
                else:
                    self.printlog('result of task {} lost (a)'.format(task))

        except (Exception, GeneratorExit) as err:
            self.printlog('TASK ERROR. {} - TRACEBACK: {}'.format(
                task, traceback.format_exc()))
            self.cleanup_terminated_task(task)

            if task_poll._send_result_to:
                dest_task = task_poll._send_result_to
                if dest_task in self._tasks:
                    self.append_late_event(
                        dest_task,
                        Event(EV_TASK_RESULT, task=task, exception=err)
                    )
                else:
                    self.printlog('result of task {} lost (b)'.format(task))


    def append_late_event(self, task, send):

        assert send is not None

        if not isinstance(task, GeneratorType):
            raise TypeError(type(task))

        self._late_events.append((task, send))


    def cleanup_terminated_task(self, task):

        task_poll = self._tasks.pop(task)

        for obj in task_poll._read:
            self._readers.pop(obj)

        for obj in task_poll._write:
            self._writers.pop(obj)

        for timer_id in task_poll._timer:
            if self._timers.pop(timer_id) is not task:
                raise AssertionError

        for name in task_poll._msgbox:
            if self._msgboxes.pop(name) is not task:
                raise AssertionError

        for thread_obj in task_poll._thread:
            if self._threads.pop(thread_obj) is not task:
                raise AssertionError

            self.printlog('terminated task', task, 'was waiting for thread', thread_obj)

        for channel_id in task_poll._broadcast:
            self._broadcast_listeners[channel_id].remove(task)
            if not self._broadcast_listeners[channel_id]:
                self._broadcast_listeners.pop(channel_id)


    def start_thread(self, func, parent_task):

        def thread_main(func, thread_id, result_queue):

            try:
                result = func()
            except Exception as err:
                result = err

            result_queue.put((thread_id, result))


        thread_id = next(self._id_generator)
        th = threading.Thread(target=thread_main, args=(func, thread_id, self._thread_result_queue))
        th.daemon = True

        self._threads[th] = parent_task
        self._threads_by_id[thread_id] = th
        self._tasks[parent_task]._thread.append(th)

        th.start()
        return thread_id


    def manage_thread_result(self, result):

        thread_id, result = result
        thread_obj = self._threads_by_id.pop(thread_id)
        parent_task = self._threads.pop(thread_obj, None)
        if parent_task:
            self.append_late_event(
                parent_task,
                Event(EV_THREAD_RESULT, thread_id=thread_id, result=result)
            )
            self._tasks[parent_task]._thread.remove(thread_obj)
        else:
            self.printlog('orphaned thread {} terminated'.format(thread_obj))


    def quit(self):
        self._w_wakeup_sock.send(self.STOP_CHAR)


class EventLoopQueue(queue.Queue):

    def __init__(self, wakeup_loop_func, id_char, maxsize=0):

        super().__init__(maxsize=maxsize)
        self.id_char = id_char
        self._wakeup_loop_func = wakeup_loop_func
        self._init_thread = threading.current_thread()


    def put(self, item, block=True, timeout=None):

        super().put({'item': item}, block, timeout)
        self._wakeup_loop_func(self.id_char)


    def put_sync(self, item, block=True, timeout=None):
        # waits until the destination EventLoop confirms the
        # delivery of the object enqueued.

        if threading.current_thread() is self._init_thread:
            raise RuntimeError('this method cannot be called in the same thread that created the EventLoopQueue object')

        wait_queue = queue.Queue()
        super().put({'item': item, 'confirm_queue': wait_queue}, block, timeout)
        self._wakeup_loop_func(self.id_char)

        confirm = wait_queue.get(block=True)
        if confirm is None:
            pass

        elif isinstance(confirm, Exception):
            raise confirm

        else:
            raise AssertionError(confirm)


# -------------------------------------------------------------------

def get_poll():
    return GET_POLL

# -------------------------------------------------------------------
def generator_position(gen_obj):

    if gen_obj.gi_frame is None:
        return '*terminated*'

    elif getattr(gen_obj, 'gi_yieldfrom', None):
        return generator_position(gen_obj.gi_yieldfrom)

    else:
        return '{}:{}'.format(
            os.path.split(gen_obj.gi_frame.f_code.co_filename)[1],
            gen_obj.gi_frame.f_lineno
        )


def describe_generator(obj):

    try:
        return '{}[{}]'.format(obj, generator_position(obj))
    except Exception:
        return '-*-'
# -------------------------------------------------------------------

if __name__ == '__main__':
    pass









