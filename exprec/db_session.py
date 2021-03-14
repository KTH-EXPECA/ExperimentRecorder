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
from queue import Queue
from typing import Any, Callable, Collection, Mapping

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

    def _make_instance(self, session: Session) -> Base:
        return self._fn(session)

    def _set_ret_attr_from_instance(self, inst: Base) -> None:
        with self._cond:
            self._result = {attr: getattr(inst, attr)
                            for attr in self._ret_attr}
            self._has_result = True
            self._cond.notify_all()

    def get_result(self) -> Mapping[str, Any]:
        with self._cond:
            while not self._has_result:
                self._cond.wait()  # TODO timeout?

        return self._result


# noinspection PyPep8Naming
class ExperimentWriterThread(threading.Thread):
    def __init__(self, db_address: str = 'sqlite:///:memory:'):
        super(ExperimentWriterThread, self).__init__()
        self._db_address = db_address

        self._urgent_ops: Queue[DBInstCreationPromise] = Queue()
        self._operations: Queue[DBInstCreationPromise] = Queue()

        self._shutdown = threading.Event()
        self._shutdown.clear()

    def eager_create(self,
                     fn: Callable[[Session], Base],
                     return_attributes: Collection[str]) -> Mapping[str, Any]:
        promise = DBInstCreationPromise(fn, return_attributes)
        self._urgent_ops.put(promise)
        return promise.get_result()

    def deferred_execute(self,
                         fn: Callable[[Session], Base],
                         return_attributes: Collection[str]) \
            -> DBInstCreationPromise:
        promise = DBInstCreationPromise(fn, return_attributes)
        self._operations.put(promise)
        return promise

    def run(self) -> None:
        # parallel execution thread.
        engine = create_engine(self._db_address)
        scoped = scoped_session(sessionmaker(bind=engine))

        while not self._shutdown.is_set():
            try:
                with session_context(scoped) as session:
