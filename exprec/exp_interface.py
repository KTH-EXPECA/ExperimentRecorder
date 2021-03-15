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
import queue
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Collection, Iterable, Mapping, Set, Tuple

from sqlalchemy.orm import Session, scoped_session, sessionmaker

from .models import *


@dataclass(frozen=True)
class VarUpdate:
    timestamp: datetime.datetime
    name: str = field(compare=False)
    value: Any = field(compare=False)
    exp_id: uuid.UUID = field(compare=False)


class _FlushThread(threading.Thread):
    def __init__(self,
                 scoped_factory: scoped_session,
                 db_lock: threading.RLock):
        super(_FlushThread, self).__init__()
        self._scoped_fact = scoped_factory
        self._db_lock = db_lock
        self._buf_queue = queue.Queue()

        self._shutdown = threading.Event()
        self._shutdown.clear()

        self._exc_lock = threading.Lock()
        self._exc = None

        # mapping from (exp_id, name) to variable table instance,
        # this is for memoization. Whenever we see a pair (exp, variable
        # name) we haven't seen before, we fetch the matching variable id
        # from the DB and save it for efficient future reference
        self._var_memo = {}

    def push_records(self, buf: Collection[VarUpdate]):
        if self._shutdown.is_set():
            raise RuntimeError('Thread is shut down.')
        else:
            self._buf_queue.put_nowait(buf)

    def flushing(self) -> bool:
        return self._buf_queue.unfinished_tasks > 0

    def wait_for_flush(self) -> None:
        self._buf_queue.join()

    def sanity_check(self) -> None:
        with self._exc_lock:
            if self._exc is not None:
                raise self._exc

    def join(self, timeout: Optional[float] = None) -> None:
        self._shutdown.set()
        self.wait_for_flush()
        super(_FlushThread, self).join(timeout=timeout)

    def _flush_to_db(self, buf: Iterable[VarUpdate]) -> None:
        # get a new session for the flush
        # this is scoped, so should be the same every time
        with self._db_lock:
            session: Session = self._scoped_fact()

        records = deque()
        for var_upd in buf:
            try:
                # memoization for efficiency
                var_id = self._var_memo[(var_upd.exp_id, var_upd.name)]
            except KeyError:
                # memoization lookup failed
                # need to find variable in database or create it!
                with self._db_lock:
                    # doing it by merge is more efficient than querying
                    # first and creating if query fails
                    variable = session.merge(InstanceVariable(
                        name=var_upd.name,
                        instance_id=var_upd.exp_id))
                    session.commit()
                    var_id = variable.id

                # memoization for future reference:
                self._var_memo[(var_upd.exp_id, var_upd.name)] = var_id

            # collect all the new records and commit them all together
            records.append(VariableRecord(variable_id=var_id,
                                          timestamp=var_upd.timestamp,
                                          value=var_upd.value))
        # commit all the records to the database
        with self._db_lock:
            session.add_all(records)
            session.commit()
            session.close()

    def run(self) -> None:
        try:
            self._shutdown.clear()
            while not self._shutdown.is_set():
                try:
                    buf = self._buf_queue.get(timeout=0.05)
                except queue.Empty:
                    continue

                self._flush_to_db(buf)
                self._buf_queue.task_done()

            # flush remaining tasks
            while True:
                try:
                    buf = self._buf_queue.get_nowait()
                    self._flush_to_db(buf)
                    self._buf_queue.task_done()
                except queue.Empty:
                    break
        except ... as e:
            with self._exc_lock:
                self._exc = e
                raise e


class BufferedExperimentInterface:
    def __init__(self,
                 db_engine: Engine,
                 buf_size: int = 100,
                 default_metadata: Mapping[str, str] = {}):
        # TODO: document

        self._engine = db_engine
        Base.metadata.create_all(self._engine)
        self._session_fact = scoped_session(sessionmaker(bind=self._engine))

        self._db_lock = threading.RLock()
        self._buf_size = buf_size
        self._buf = deque()  # no max size, max buf_size is only minimum size
        # to flush automatically

        self._session: Session = self._session_fact()

        # collect all the experiment instances we make
        self._experiment_ids: Set[uuid.UUID] = set()

        # flush in a separate thread for efficiency
        self._flush_t = _FlushThread(scoped_factory=self._session_fact,
                                     db_lock=self._db_lock)
        self._flush_t.start()

        # shortcuts
        self.sanity_check = self._flush_t.sanity_check
        self.wait_for_flush = self._flush_t.wait_for_flush

        # default metadata for all instances
        self._def_metadata = default_metadata

    @property
    def experiment_instances(self) -> Tuple[uuid.UUID]:
        return tuple(self._experiment_ids)

    @property
    def session(self) -> Session:
        return self._session

    def flush(self, blocking: bool = False):
        # flush buffer to disk
        buf = tuple(self._buf)
        self._buf.clear()
        self._flush_t.push_records(buf)
        if blocking:
            self._flush_t.wait_for_flush()
            self._flush_t.sanity_check()

    def close(self):
        self.flush(blocking=True)
        self._flush_t.join()
        self._session.commit()
        self._session.close()

    def new_experiment_instance(self) -> uuid.UUID:
        experiment = ExperimentInstance()
        with self._db_lock:
            self._session.add(experiment)
            self._session.commit()

            exp_id = experiment.id

            # TODO: test
            # add the default metadata to the new instance
            self.add_metadata(exp_id, **self._def_metadata)
            self._experiment_ids.add(exp_id)

            return exp_id

    def finish_experiment_instance(self, exp_id: uuid.UUID):
        with self._db_lock:
            exp = self._session \
                .query(ExperimentInstance) \
                .filter(ExperimentInstance.id == exp_id) \
                .first()
            exp.end = datetime.datetime.now()
            self._session.commit()

    def add_metadata(self, experiment_id: uuid.UUID, **kwargs) -> None:
        metadata = [ExperimentMetadata(instance_id=experiment_id,
                                       label=k, value=v)
                    for k, v in kwargs.items()]
        with self._db_lock:
            for mdata in metadata:
                # merge to update keys that may potentially already exist in db
                self._session.merge(mdata)
            self._session.commit()

    def record_variables(self,
                         experiment_id: uuid.UUID,
                         timestamp: datetime.datetime,
                         **kwargs):
        self._buf.extend([
            VarUpdate(timestamp=timestamp, exp_id=experiment_id,
                      name=var, value=val, ) for var, val in kwargs.items()])

        # flush to disk if buffered enough
        if len(self._buf) >= self._buf_size:
            self.flush()
