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
import datetime
import json
import os
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Collection, Dict, Mapping

import numpy as np
import pandas as pd
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.endpoints import clientFromString, serverFromString
from twisted.internet.interfaces import IListeningPort
from twisted.internet.posixbase import PosixReactorBase
from twisted.internet.protocol import Factory
from twisted.logger import Logger, eventAsText, globalLogPublisher
from twisted.trial import unittest

from exprec.client.client import ExperimentClient
from exprec.server.protocol import ExperimentRecordingServer

reactor: PosixReactorBase = reactor

default_metadata = {
    'name'       : 'test_experiment',
    'description': 'Test experiment, full integration.'
}

socket_conn = 'unix:/tmp/exprec.sock'


@dataclass(frozen=True)
class VarRecord:
    timestamp: datetime.datetime
    variables: Mapping[str, Any]


@dataclass(frozen=True)
class ExperimentTestData:
    records: Collection[VarRecord]
    metadata: Mapping[str, str]
    default_metadata: Mapping[str, str] \
        = field(init=False, default_factory=lambda: default_metadata)


# noinspection DuplicatedCode
class TestClientServer(unittest.TestCase):
    # full interaction test
    def setUp(self) -> None:
        # add a logger to check what's going on
        def log_observer(e):
            print(eventAsText(e))

        self._obs = log_observer
        globalLogPublisher.addObserver(self._obs)
        self._log = Logger()

        self._out_path = Path('/tmp/exprec_test')

        self._experiments: Dict[uuid.UUID, ExperimentTestData] = dict()

        # set up the server, listening on a UNIX socket
        self._server = ExperimentRecordingServer(
            db_path=':memory:',
            db_persist=True,
            output_dir=self._out_path,
            default_metadata=default_metadata
        )

        self._endpoint = serverFromString(reactor, socket_conn)

        def listening(port: IListeningPort):
            self._log.info('Server listening and ready.')
            self._port = port

        return self._endpoint.listen(self._server).addCallback(listening)

    def tearDown(self) -> None:
        def after_server_shutdown(_):
            # remove the observer!!
            globalLogPublisher.removeObserver(self._obs)

            files_in_output = os.listdir(self._out_path)

            self.assertIn(self._server.records_path.name, files_in_output)
            self.assertIn(self._server.metadata_path.name, files_in_output)
            self.assertIn(self._server.times_path.name, files_in_output)

            records = pd.read_csv(self._server.records_path)
            if not records.empty:
                records['timestamp'] = pd.to_datetime(records['timestamp'])
                records['experiment'] = records['experiment'].map(uuid.UUID)

            with self._server.metadata_path.open('r') as fp:
                metadata = json.load(fp)

            with self._server.times_path.open('r') as fp:
                times = json.load(fp)

            for exp_id, test_data in self._experiments.items():
                for var_rec in test_data.records:
                    for k, v in var_rec.variables.items():
                        rec_val = records[
                            (records['timestamp'] == var_rec.timestamp) &
                            (records['experiment'] == exp_id)
                            ][k]
                        # isclose to deal with float precision
                        self.assertTrue(np.isclose(rec_val, v))

                # check the stored metadata
                exp_id = str(exp_id)
                for k, v in test_data.default_metadata.items():
                    self.assertIn(exp_id, metadata)
                    self.assertIn(k, metadata[exp_id])
                    self.assertEqual(metadata[exp_id][k], v)

                for k, v in test_data.metadata.items():
                    self.assertIn(exp_id, metadata)
                    self.assertIn(k, metadata[exp_id])
                    self.assertEqual(metadata[exp_id][k], v)

                # check the stored times
                # all experiments in these runs should have both start and
                # end times
                self.assertIn(exp_id, times)
                self.assertIn('start', times[exp_id])
                self.assertIn('end', times[exp_id])

                try:
                    datetime.datetime.fromisoformat(times[exp_id]['start'])
                except ValueError:
                    self.fail(f'Could not parse start time from '
                              f'{times[exp_id]}.')

                try:
                    datetime.datetime.fromisoformat(times[exp_id]['end'])
                except ValueError:
                    self.fail(f'Could not parse end time from '
                              f'{times[exp_id]}.')

        return self._port.stopListening().addCallback(after_server_shutdown)

    def test_client_connect_disconnect(self):
        # simple client connection + disconnection test. no additional data
        # is sent

        wait_d = Deferred()

        class TestClient(ExperimentClient):
            testcase = self

            def got_experiment_id(self, experiment_id: uuid.UUID):
                super(TestClient, self).got_experiment_id(experiment_id)
                self.testcase._experiments[experiment_id] = ExperimentTestData(
                    records=[], metadata={}
                )
                wait_d.callback(self)

        def disconnect(client: TestClient):
            return client.finish()

        wait_d.addCallback(disconnect)

        endpoint = clientFromString(reactor, socket_conn)
        endpoint \
            .connect(Factory.forProtocol(TestClient)) \
            .addCallback(lambda _: self._log.info('Client connected.'))

        return wait_d

    def test_send_metadata(self):
        # simple client connection + send metadata + disconnection test.
        metadata_to_send = {'test': 'metadata', 'foo': 'bar'}

        wait_d = Deferred()

        class TestClient(ExperimentClient):
            testcase = self

            def got_experiment_id(self, experiment_id: uuid.UUID):
                super(TestClient, self).got_experiment_id(experiment_id)
                self.testcase._experiments[experiment_id] = ExperimentTestData(
                    records=[], metadata=metadata_to_send
                )
                wait_d.callback(self)

        def ready(client: TestClient):
            # send metadata and wait for ack before disconnecting
            self._log.info('Client sending metadata.')
            return client \
                .write_metadata(**metadata_to_send) \
                .addCallback(lambda _: client.finish())

        endpoint = clientFromString(reactor, socket_conn)
        endpoint.connect(Factory.forProtocol(TestClient)) \
            .addCallback(lambda _: self._log.info('Client connected.'))

        wait_d.addCallback(ready)
        return wait_d

    def test_send_records(self):
        # client connection + send records + disconnection test.
        records = deque([
            VarRecord(
                timestamp=datetime.datetime.fromtimestamp(i),
                variables={
                    'a': np.random.uniform(),
                    'b': np.random.normal(),
                    'c': np.random.randint(0, 1000)
                })
            for i in np.logspace(1, 9, num=10000)
        ])

        wait_d = Deferred()

        class TestClient(ExperimentClient):
            testcase = self

            def got_experiment_id(self, experiment_id: uuid.UUID):
                super(TestClient, self).got_experiment_id(experiment_id)
                self.testcase._experiments[experiment_id] = ExperimentTestData(
                    records=records, metadata={}
                )
                wait_d.callback(self)

        def ready(client: TestClient):
            # send metadata and wait for ack before disconnecting
            self._log.info('Client sending records.')

            def send(_):
                try:
                    rec_to_send = records.pop()
                    return client.record_variables(
                        timestamp=rec_to_send.timestamp,
                        **rec_to_send.variables
                    ).addCallback(send)
                except IndexError:
                    return client.finish()

            return send(None)

        endpoint = clientFromString(reactor, socket_conn)
        endpoint.connect(Factory.forProtocol(TestClient)) \
            .addCallback(lambda _: self._log.info('Client connected.'))

        wait_d.addCallback(ready)
        return wait_d
