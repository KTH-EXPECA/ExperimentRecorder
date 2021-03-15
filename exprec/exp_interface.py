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
from dataclasses import dataclass, field
from typing import Any, Set, Tuple

from sqlalchemy.orm import Session, scoped_session, sessionmaker

from .models import *


@dataclass(frozen=True)
class VarUpdate:
    timestamp: datetime.datetime
    name: str = field(compare=False)
    value: Any = field(compare=False)
    exp_id: uuid.UUID = field(compare=False)


class BufferedExperimentInterface:
    def __init__(self,
                 db_engine: Engine,
                 buf_size: int = 100):
        # self._engine = create_engine(db_address,
        #                              connect_args={'check_same_thread':
        #                              False},
        #                              poolclass=poolclass)
        self._engine = db_engine
        Base.metadata.create_all(self._engine)
        self._session_fact = scoped_session(sessionmaker(bind=self._engine))

        self._db_lock = threading.Lock()
        self._buf_size = buf_size
        self._buf = deque()  # no max size, max buf_size is only minimum size
        # to flush automatically

        self._session: Session = self._session_fact()

        self._flush_cond = threading.Condition()
        self._flush_done = True

        self._exc_lock = threading.Lock()
        self._exc = None

        # collect all the experiment instances we make
        self._experiment_ids: Set[uuid.UUID] = set()

    @property
    def experiment_instances(self) -> Tuple[uuid.UUID]:
        return tuple(self._experiment_ids)

    @property
    def session(self) -> Session:
        return self._session

    def sanity_check(self) -> None:
        with self._exc_lock:
            if self._exc is not None:
                raise self._exc

    def wait_for_flush(self) -> None:
        with self._flush_cond:
            if not self._flush_done:
                self._flush_cond.wait()

    def flush(self, blocking: bool = False):
        # flush buffer to disk
        buf = tuple(self._buf)
        self._buf.clear()

        def _flush_thread():
            with self._flush_cond:
                self._flush_done = False

            # get a new session for the thread
            session: Session = self._session_fact()

            # mapping from (exp_id, name) to variable table instance
            variable_map = {}

            records = deque()
            for var_upd in buf:
                try:
                    var_id = variable_map[(var_upd.exp_id, var_upd.name)]
                except KeyError:
                    # memoization lookup failed
                    # need to find variable in database or create it!
                    with self._db_lock:
                        variable = session.query(InstanceVariable) \
                            .filter_by(instance_id=var_upd.exp_id,
                                       name=var_upd.name) \
                            .first()
                        if variable is None:
                            variable = InstanceVariable(
                                name=var_upd.name,
                                instance_id=var_upd.exp_id)
                            session.add(variable)
                            session.commit()

                        var_id = variable.id

                    variable_map[(var_upd.exp_id, var_upd.name)] = var_id

                records.append(VariableRecord(variable_id=var_id,
                                              timestamp=var_upd.timestamp,
                                              value=var_upd.value))
            # commit all the records to the database
            with self._db_lock:
                session.add_all(records)
                session.commit()
                session.close()

            with self._flush_cond:
                self._flush_done = True
                self._flush_cond.notify_all()

        # fire off thread, but first wait for previous thread to finish
        self.wait_for_flush()
        # check for possible exceptions in previous flush
        self.sanity_check()
        threading.Thread(target=_flush_thread).start()

        if blocking:
            self.wait_for_flush()
            # check for exceptions
            self.sanity_check()

    def close(self):
        self.flush(blocking=True)
        self._session.commit()
        self._session.close()

    def new_experiment_instance(self) -> uuid.UUID:
        experiment = ExperimentInstance()
        with self._db_lock:
            self._session.add(experiment)
            self._session.commit()

            self._experiment_ids.add(experiment.id)
            return experiment.id

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
