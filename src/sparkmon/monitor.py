# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""Monitor thread."""
import concurrent.futures
import threading
import time
import urllib
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

import sparkmon


class SparkMon(threading.Thread):
    """Class to manage a socket in a background thread to talk to the scala listener.

    This is a daemon thread, meaning that it is not blocking the exit of the application,
    and can be abruptly stopped at shutdown.
    But it is running the callbacks into a ThreadPool that is blocking,
    meaning that they need to terminate to let the application exit.
    Indeed, it is important to not run the callbacks in the daemon,
    so that a file export callback wouldn't be interupted in the middle of saving.

    In case of problem, we could do another architecture: just one thread, this one,
    with deamon=False, and using 'atexit' to safely end the monitoring and not blocking the app.
    """

    def __init__(
        self,
        application: sparkmon.Application,
        period: int = 20,
        callbacks: Optional[List[Callable[..., Any]]] = None,
    ) -> None:
        """Constructor, initializes base class Thread."""
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.daemon = True
        self.cnt = 0
        self.application = application
        self.application_lock = threading.Lock()
        self.period = period
        if callbacks is None:
            callbacks = []
        self.callbacks = callbacks
        self.updateEvent = threading.Event()
        self.callbacks_pool = concurrent.futures.ThreadPoolExecutor()
        self.callbacks_future = None

    def stop(self) -> None:
        """To stop the thread."""
        self._stop.set()

    def stopped(self) -> bool:
        """Overrides Thread method."""
        return self._stop.isSet()

    def run(self) -> None:
        """Overrides Thread method."""
        while True:
            if self.stopped():
                return

            ###
            # Updating the application DB at the regular period:
            try:
                # Callbacks are reading application, so let's make thread safe with a lock:
                with self.application_lock:
                    self.application.log_all()
                    self.cnt += 1
            except urllib.error.URLError as ex:
                if self.cnt > 1:
                    print(f"sparkmon: Spark application not available anymore. Exception: {ex}")
                    self.stop()
                    return

            ###
            # Callback can be run at a slower pace, specially if they are slow/expensive:

            # Check if callbacks are still running
            # TODO: check if using .done() or add_done_callback() is not better then .running()?
            if self.callbacks_future is not None and not self.callbacks_future.running():
                self.callbacks_future = None

            # We only run the callbacks if they are not running
            if self.callbacks_future is None:
                self.callbacks_future = self.callbacks_pool.submit(self.callbacks_run)

            self.updateEvent.set()
            time.sleep(self.period)

    def callbacks_run(self):
        """Running the callbacks."""
        for callback in self.callbacks:
            with self.application_lock:
                callback(self.application)

    def live_plot_notebook(self, n_iter=None) -> None:
        """Useful in the remote case only."""
        cnt = 0
        while True:
            cnt += 1
            if n_iter is not None and cnt > n_iter:
                return
            if self.stopped():
                return

            sparkmon.plot_notebook(self.application)

            self.updateEvent.clear()
            self.updateEvent.wait()

    def __enter__(self):
        """Start thread in contextmanager."""
        self.start()
        return self

    def __exit__(self, *args, **kwargs):
        """Stop thread in contextmanager."""
        self.stop()
