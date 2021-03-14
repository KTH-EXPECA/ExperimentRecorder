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
from collections import deque
from queue import Empty, Queue
from typing import Any, Callable, Collection, Mapping, Optional

from contextlib2 import contextmanager
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import Session, scoped_session, sessionmaker

# noinspection PyPep8Naming
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
            while not self._has_result:
                self._cond.wait(timeout=timeout)

        return self._result


# noinspection PyPep8Naming
class ExperimentWriterThread(threading.Thread):
    def __init__(self,
                 db_address: str = 'sqlite:///:memory:',
                 buf_size: int = 100):
        super(ExperimentWriterThread, self).__init__()
        self._db_address = db_address
        self._buf_size = buf_size

        self._urgent_ops: Queue[DBInstCreationPromise] = Queue()
        self._operations: Queue[DBInstCreationPromise] = Queue()

        self._shutdown = threading.Event()
        self._shutdown.clear()

        self._flush_lock = threading.RLock()
        self._t_exception = None

    def eager_create(self,
                     fn: Callable[[Session], Base],
                     return_attributes: Collection[str]) -> Mapping[str, Any]:
        with self._flush_lock:
            if self._shutdown.is_set():
                raise RuntimeError('DB session is shut down.')

            promise = DBInstCreationPromise(fn, return_attributes)
            self._urgent_ops.put(promise)
            return promise.get_result()

    def deferred_execute(self,
                         fn: Callable[[Session], Base],
                         return_attributes: Collection[str]) \
            -> DBInstCreationPromise:
        with self._flush_lock:
            if self._shutdown.is_set():
                raise RuntimeError('DB session is shut down.')

            promise = DBInstCreationPromise(fn, return_attributes)
            self._operations.put(promise)
            return promise

    def sanity_check(self) -> None:
        with self._flush_lock:
            if self._t_exception is not None:
                raise self._t_exception

    def shutdown(self) -> None:
        self._shutdown.set()
        self.join()
        self.sanity_check()

    # noinspection PyProtectedMember
    def run(self) -> None:
        # parallel execution thread.
        engine = create_engine(self._db_address)
        scoped = scoped_session(sessionmaker(bind=engine))
        op_buf = deque(maxlen=self._buf_size)
        try:
            while not self._shutdown.is_set():
                try:
                    urgent_op = self._urgent_ops.get(timeout=0.01)
                    # if we get an urgent op, execute it immediately
                    with session_context(scoped) as session:
                        inst = urgent_op._make_instance(session)
                        session.add(inst)
                        session.commit()

                        # register the results
                        urgent_op._set_ret_attr_from_instance(inst)
                except Empty:
                    # if no urgent ops, check if deferred ops
                    rem_buf_space = op_buf.maxlen - len(op_buf)
                    for i in range(rem_buf_space):
                        try:
                            op_buf.append(self._operations.get_nowait())
                        except Empty:
                            break

                    if len(op_buf) >= op_buf.maxlen:
                        # flush op_buf to db
                        with session_context(scoped) as session:
                            instances = [op._make_instance(session)
                                         for op in op_buf]
                            session.add_all(instances)
                            session.commit()

                            for promise, inst in zip(op_buf, instances):
                                promise._set_ret_attr_from_instance(inst)
                        op_buf.clear()

            with self._flush_lock:
                self._shutdown.set()
                # flush whatever is left
                try:
                    # start with urgent ops
                    while True:
                        urgent_op = self._urgent_ops.get_nowait()
                        # if we get an urgent op, execute it immediately
                        with session_context(scoped) as session:
                            inst = urgent_op._make_instance(session)
                            session.add(inst)
                            session.commit()

                            # register the results
                            urgent_op._set_ret_attr_from_instance(inst)
                except Empty:
                    pass

                # next, non-urgent
                # first, deque then directly from queue
                with session_context(scoped) as session:
                    promises = [op for op in op_buf]
                    instances = [op._make_instance(session) for op in promises]
                    op_buf.clear()

                    try:
                        promise = self._operations.get_nowait()
                        promises.append(promise)
                        instances.append(promise._make_instance(session))
                    except Empty:
                        pass

                    session.add_all(instances)
                    session.commit()

                    for promise, inst in zip(promises, instances):
                        promise._set_ret_attr_from_instance(inst)
        except Exception as e:
            with self._flush_lock:
                self._shutdown.set()
                self._t_exception = e
                raise e
