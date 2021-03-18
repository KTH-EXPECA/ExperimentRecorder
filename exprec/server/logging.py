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
from typing import Any, Mapping, Optional

import loguru
from twisted.logger import LogLevel, eventAsText, globalLogPublisher

_level_mapping = {
    LogLevel.debug   : loguru.logger.debug,
    LogLevel.info    : loguru.logger.info,
    LogLevel.warn    : loguru.logger.warning,
    LogLevel.error   : loguru.logger.error,
    LogLevel.critical: loguru.logger.critical,
}


class LoggingThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self._events = queue.Queue()
        self._shutdown = threading.Event()
        self._shutdown.clear()

        # set up as observer
        globalLogPublisher.addObserver(self.emit)

    def join(self, timeout: Optional[float] = None) -> None:
        self._shutdown.set()
        # can't use join because it doesn't have a timeout.
        # self._events.join()

        with self._events.all_tasks_done:
            while self._events.unfinished_tasks:
                self._events.all_tasks_done.wait(timeout=timeout)

        super(LoggingThread, self).join(timeout)

    def emit(self, event: Mapping[str, Any]) -> None:
        if self._shutdown.is_set():
            raise RuntimeError('Logging thread is not running!')

        self._events.put_nowait(event)

    def run(self) -> None:
        self._shutdown.clear()
        while not self._shutdown.is_set():
            try:
                event = self._events.get(timeout=0.1)
            except queue.Empty:
                continue

            level = event.get('log_level', LogLevel.info)
            _level_mapping[level](eventAsText(event))
            self._events.task_done()

        while True:
            try:
                event = self._events.get_nowait()
                level = event.get('log_level', LogLevel.info)
                _level_mapping[level](eventAsText(event))
                self._events.task_done()
            except queue.Empty:
                break
