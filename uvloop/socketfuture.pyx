# Internal socket poll waiter.  Loop.create_future() continues to use
# the standard asyncio.Future for public futures.

cdef enum _SocketFutureState:
    _SOCKET_PENDING = 0
    _SOCKET_CANCELLED = 1
    _SOCKET_FINISHED = 2


@cython.final
@cython.freelist(256)
cdef class _SocketFuture:
    cdef:
        object __weakref__
        object _loop
        object _sock
        object _result
        object _exception
        object _exception_tb
        object _cancel_message
        object _source_traceback
        list _callbacks
        int _state
        bint _writer
        bint _log_exception
        public object _asyncio_future_blocking

    def __cinit__(self):
        self._state = _SOCKET_PENDING
        self._asyncio_future_blocking = False
        self._callbacks = []
        self._log_exception = False

    def __init__(self, sock, loop, bint writer=False):
        self._sock = sock
        self._loop = loop
        self._writer = writer
        if loop.get_debug():
            self._source_traceback = asyncio.format_helpers.extract_stack(
                sys._getframe(1))

    def __repr__(self):
        cdef str state
        if self._state == _SOCKET_PENDING:
            state = 'pending'
        elif self._state == _SOCKET_CANCELLED:
            state = 'cancelled'
        else:
            state = 'finished'
        return f'<{type(self).__name__} {state}>'

    @property
    def _state_string(self):
        if self._state == _SOCKET_PENDING:
            return 'PENDING'
        if self._state == _SOCKET_CANCELLED:
            return 'CANCELLED'
        return 'FINISHED'

    @property
    def _log_traceback(self):
        return self._log_exception

    @_log_traceback.setter
    def _log_traceback(self, val):
        if val:
            raise ValueError('_log_traceback can only be set to False')
        self._log_exception = False

    def __del__(self):
        if not self._log_exception:
            return
        context = {
            'message': f'{type(self).__name__} exception was never retrieved',
            'exception': self._exception,
            'future': self,
        }
        if self._source_traceback is not None:
            context['source_traceback'] = self._source_traceback
        self._loop.call_exception_handler(context)

    def get_loop(self):
        return self._loop

    cdef inline _remove_poll(self):
        cdef object sock = self._sock
        self._sock = None
        if sock is not None and sock.fileno() != -1:
            if self._writer:
                (<Loop>self._loop)._remove_writer(sock)
            else:
                (<Loop>self._loop)._remove_reader(sock)

    cdef inline _schedule_callbacks(self):
        cdef list callbacks = self._callbacks
        cdef object cb
        cdef object ctx
        self._callbacks = None
        for cb, ctx in callbacks:
            (<Loop>self._loop)._call_soon(cb, (self,), ctx)

    cpdef bint cancel(self, object msg=None):
        self._log_exception = False
        if self._state != _SOCKET_PENDING:
            return False
        self._state = _SOCKET_CANCELLED
        self._cancel_message = msg
        try:
            self._remove_poll()
        finally:
            self._schedule_callbacks()
        return True

    cpdef bint cancelled(self):
        return self._state == _SOCKET_CANCELLED

    cpdef bint done(self):
        return self._state != _SOCKET_PENDING

    def result(self):
        if self._state == _SOCKET_CANCELLED:
            if self._cancel_message is not None:
                raise asyncio.CancelledError(self._cancel_message)
            raise asyncio.CancelledError()
        if self._state != _SOCKET_FINISHED:
            raise asyncio.InvalidStateError('Result is not ready.')
        self._log_exception = False
        if self._exception is not None:
            raise self._exception.with_traceback(self._exception_tb)
        return self._result

    def exception(self):
        if self._state == _SOCKET_CANCELLED:
            if self._cancel_message is not None:
                raise asyncio.CancelledError(self._cancel_message)
            raise asyncio.CancelledError()
        if self._state != _SOCKET_FINISHED:
            raise asyncio.InvalidStateError('Exception is not set.')
        self._log_exception = False
        return self._exception

    def add_done_callback(self, fn, *, context=None):
        if self._state != _SOCKET_PENDING:
            (<Loop>self._loop)._call_soon(fn, (self,), context)
        else:
            if context is None:
                context = Context_CopyCurrent()
            self._callbacks.append((fn, context))

    def remove_done_callback(self, fn):
        if self._callbacks is None:
            return 0
        cdef Py_ssize_t before = len(self._callbacks)
        self._callbacks = [(f, ctx) for f, ctx in self._callbacks if f != fn]
        return before - len(self._callbacks)

    cpdef set_result(self, object result):
        if self._state != _SOCKET_PENDING:
            raise asyncio.InvalidStateError(
                f'{self._state_string}: {self!r}')
        self._result = result
        self._state = _SOCKET_FINISHED
        self._schedule_callbacks()

    cpdef set_exception(self, object exception):
        if self._state != _SOCKET_PENDING:
            raise asyncio.InvalidStateError(
                f'{self._state_string}: {self!r}')
        if isinstance(exception, type):
            exception = exception()
        if not isinstance(exception, BaseException):
            raise TypeError('invalid exception object')
        if type(exception) is StopIteration:
            raise TypeError('StopIteration interacts badly with generators '
                            'and cannot be raised into a Future')
        self._exception = exception
        self._exception_tb = exception.__traceback__
        self._state = _SOCKET_FINISHED
        self._schedule_callbacks()
        self._log_exception = True

    def __await__(self):
        if self._state == _SOCKET_PENDING:
            self._asyncio_future_blocking = True
            yield self
        if self._state == _SOCKET_PENDING:
            raise RuntimeError("await wasn't used with future")
        return self.result()
