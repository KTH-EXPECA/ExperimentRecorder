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
import uuid

import click
import numpy as np
from twisted.internet import reactor, task
from twisted.internet.endpoints import clientFromString
from twisted.internet.posixbase import PosixReactorBase
from twisted.internet.protocol import Factory
from twisted.internet.task import LoopingCall
from twisted.logger import eventAsText, globalLogPublisher

from exprec.client.client import ExperimentClient

reactor: PosixReactorBase = reactor


@click.command()
@click.argument('endpoint', type=str)
@click.option('-t', '--timeout', type=float,
              default=10.0, help='How long to run this for, in seconds.',
              show_default=True)
@click.help_option()
def main(endpoint: str, timeout: float):
    # logger
    globalLogPublisher.addObserver(lambda e: print(eventAsText(e)))

    class ExampleClient(ExperimentClient):
        def got_experiment_id(self, experiment_id: uuid.UUID):
            super(ExampleClient, self).got_experiment_id(experiment_id)

            # set up a looping call and send records
            def send_record():
                timestamp = datetime.datetime.now()
                var_a = np.random.normal()
                var_b = np.random.normal()
                self.record_variables(timestamp, a=var_a, b=var_b)

            self._logger.info('Starting LoopingCall.')
            lc = LoopingCall(send_record)
            lc.start(0.10)  # 10 records per second

            self._logger.info('Scheduling shutdown for {secs}s in the future.',
                              secs=timeout)

            task.deferLater(reactor, timeout, lc.stop) \
                .addCallback(lambda _: self.finish()) \
                .addCallback(lambda _: reactor.stop())

    endpoint = clientFromString(reactor, endpoint)
    endpoint.connect(Factory.forProtocol(ExampleClient))
    reactor.run()


if __name__ == '__main__':
    main()
