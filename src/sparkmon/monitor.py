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
import threading
import time
import urllib
from typing import Any
from typing import Callable
from typing import List
from typing import Optional

import sparkmon


class SparkMon(threading.Thread):
    """Class to monitor a Spark application, running in the background.

    There are multiple design patterns possible for this class.
    One design possibility is to use 2 threads:
    - a daemon thread for the regular update (non-blocking the exit)
    - a non-daemon thread to run the callbacks (blocking the exit)
    Indeed, in this design, it is important to not run the callbacks in the daemon,
    so that a file export callbacks wouldn't be interrupted in the middle of saving.
    Here are the advantage and disadvantage:
    + you can run the callbacks at a slower pace
    - it complexifies at lot and it creates a lot problem like race conditions and dead lock.
    (we had this design at version 0.0.4)

    This is why we took the decision to use only 1 non-daemon thread and check the MainThread status,
    to smoothly stop the monitoring at exit.

    Remark: The same 'application' should not be updated by something else, like other SparkMon instances,
    because it could create race conditions.
    """

    def __init__(
        self,
        application: sparkmon.Application,
        period: int = 20,
        callbacks: Optional[List[Callable[..., Any]]] = None,
    ) -> None:
        """Constructor, initializes base class Thread."""
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.cnt = 0
        self.application = application
        self.application_lock = threading.Lock()
        self.period = period
        if callbacks is None:
            callbacks = []
        self.callbacks = callbacks
        self.updateEvent = threading.Event()

    def stop(self) -> None:
        """Don't continue to run the loop, and exit safely the thread."""
        self.stop_event.set()

    def stopped(self) -> bool:
        """Check if we need to stop."""
        return self.stop_event.isSet()

    def is_main_thread_alive(self) -> bool:
        """Check if the main thread is alive."""
        for t in threading.enumerate():
            if t.name == "MainThread":
                return t.is_alive()

    def run(self) -> None:
        """Overrides Thread method."""
        while True:
            if self.stopped():
                return

            # This is a Thread class (non daemon) meaning it can run for ever and block the exit of Python at the end.
            # This is why we check if the main thread is finished to stop SparkMon in a smooth manner at exit:
            if not self.is_main_thread_alive():
                self.stop()

            # Updating the application DB
            try:
                # Callbacks are reading application, so let's make thread safe with a lock:
                with self.application_lock:
                    self.application.log_all()
                    self.cnt += 1
            except urllib.error.URLError as ex:
                # Continue to wait for the start of the app
                if self.cnt > 1:
                    # Not need to print if we exited or stopped
                    if not self.stopped():
                        print(
                            f"sparkmon: Info, Spark application not available anymore, stopping monitoring. (Exception: {ex})"
                        )
                    return

            # Run the callback
            self.callbacks_run()

            if self.stopped():
                return

            self.updateEvent.set()
            time.sleep(self.period)

    def callbacks_run(self):
        """Running the callbacks."""
        for callback in self.callbacks:
            with self.application_lock:
                callback(self.application)

    def live_plot_notebook(self, n_iter=None) -> None:
        """Useful in the remote case only.

        This might not be compatible with callbacks that are using matplotlib, because matplotlib is not thread safe,
        and you can get the following errors:
        ```
        python(81469,0x1106c5e00) malloc: Incorrect checksum for freed object 0x7fe18da140a8: probably modified after being freed.
        Corrupt value: 0x230017000b00f005
        python(81469,0x1106c5e00) malloc: *** set a breakpoint in malloc_error_break to debug
        ```
        """
        cnt = 0
        while True:
            cnt += 1
            if n_iter is not None and cnt > n_iter:
                return
            if self.stopped():
                return

            with self.application_lock:
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
