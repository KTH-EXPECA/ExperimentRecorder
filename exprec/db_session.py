#  Copyright (c) 2021 KTH Royal Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from queue import Empty, PriorityQueue, Queue
from typing import Any, Callable, Collection, Iterable, Mapping, Optional

from contextlib2 import contextmanager
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from .models import Base


@contextmanager
def session_context(scoped: scoped_session) -> Session:
    """Provide a transactional scope around a series of operations."""
    session = scoped()
    try:
        yield session
        session.commit()
        # session.flush()
    except:
        session.rollback()
        raise
    finally:
        session.close()


class DBInstCreationPromise:
    def __init__(self,
                 fn: Callable[[Session], Base],
                 return_attributes: Collection[str]):
        self._fn = fn
        self._ret_attr = return_attributes

        self._cond = threading.Condition()
        self._has_result = False
        self._result = None

    @property
    def has_result(self) -> bool:
        with self._cond:
            return self._has_result

    def _make_instance(self, session: Session) -> Base:
        return self._fn(session)

    def _set_ret_attr_from_instance(self, inst: Base) -> None:
        with self._cond:
            self._result = {attr: getattr(inst, attr)
                            for attr in self._ret_attr}
            self._has_result = True
            self._cond.notify_all()

    def get_result(self, timeout: Optional[float] = None) -> Mapping[str, Any]:
        with self._cond:
            if not self._has_result:
                self._cond.wait(timeout=timeout)
                if not self._has_result:
                    # timeout
                    raise TimeoutError('Timed out waiting for Promise result.')
                else:
                    return self._result


@dataclass(frozen=True)
class _PrioQueueElement:
    priority: int
    timestamp: float
    promise: DBInstCreationPromise = field(compare=False)


# noinspection PyPep8Naming
class ExperimentWriterThread(threading.Thread):
    urgent_prio = 0
    relaxed_prio = 100

    def __init__(self,
                 db_address: str = 'sqlite:///:memory:',
                 buf_size: int = 100):
        super(ExperimentWriterThread, self).__init__()
        self._db_address = db_address
        self._buf_size = buf_size

        # use a priority queue to always yield urgent instances first
        self._instances: Queue[_PrioQueueElement] = PriorityQueue()

        self._shutdown = threading.Event()
        self._shutdown.clear()

        self._lock = threading.Lock()
        self._t_exception = None

    def urgent_create(self,
                      fn: Callable[[Session], Base],
                      return_attributes: Collection[str],
                      timeout: float = 0.1) -> Mapping[str, Any]:
        promise = DBInstCreationPromise(fn, return_attributes)
        self._instances.put(_PrioQueueElement(
            priority=self.urgent_prio,
            timestamp=time.monotonic(),
            promise=promise
        ))
        return promise.get_result(timeout=timeout)

    def relaxed_create(self,
                       fn: Callable[[Session], Base],
                       return_attributes: Collection[str]) \
            -> DBInstCreationPromise:
        promise = DBInstCreationPromise(fn, return_attributes)
        self._instances.put(_PrioQueueElement(
            priority=self.relaxed_prio,
            timestamp=time.monotonic(),
            promise=promise
        ))
        return promise

    def sanity_check(self) -> None:
        with self._lock:
            if self._t_exception is not None:
                raise self._t_exception

    def shutdown(self) -> None:
        self._shutdown.set()
        self.join()
        self.sanity_check()

    def run(self) -> None:
        # parallel execution thread.
        engine = create_engine(self._db_address)
        scoped = scoped_session(sessionmaker(bind=engine))
        op_buf = deque(maxlen=self._buf_size)

        # noinspection PyProtectedMember
        def _add_commit_all(promises: Iterable[DBInstCreationPromise]) \
                -> None:
            with session_context(scoped) as sess:
                instances = [p._make_instance(sess) for p in promises]
                sess.add_all(instances)
                sess.commit()

                for promise, instance in zip(promises, instances):
                    promise._set_ret_attr_from_instance(instance)

        try:
            while not self._shutdown.is_set():
                try:
                    q_elem = self._instances.get(timeout=0.1)
                except Empty:
                    continue

                if q_elem.priority == self.urgent_prio:
                    # execute immediately
                    _add_commit_all([q_elem.promise])
                else:
                    # if not urgent, put into buffer, then check buffer
                    # size to decide if it's time to execute
                    op_buf.append(q_elem.promise)
                    if len(op_buf) >= op_buf.maxlen:
                        # flush op_buf to db
                        _add_commit_all(op_buf)
                        op_buf.clear()

            # flush whatever is left
            rem = deque(op_buf)
            op_buf.clear()
            try:
                while True:
                    rem.append(self._instances.get_nowait())
            except Empty:
                pass

            _add_commit_all(rem)
            rem.clear()

        except Exception as e:
            self._shutdown.set()
            with self._lock:
                self._t_exception = e
                raise e
