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
    """Class to manage a socket in a background thread to talk to the scala listener."""

    def __init__(
        self,
        application: sparkmon.Application,
        period: int = 20,
        callbacks: Optional[List[Callable[..., Any]]] = None,
    ) -> None:
        """Constructor, initializes base class Thread."""
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.cnt = 0
        self._application = application
        self.period = period
        if callbacks is None:
            callbacks = []
        self.callbacks = callbacks
        self.updateEvent = threading.Event()

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

            try:
                self._application.log_executors_info()
                self.cnt += 1
            except urllib.error.URLError as ex:
                if self.cnt > 1:
                    print(
                        f"sparkmon: Spark application not available anymore. Exception: {ex}"
                    )
                    self.stop()
                    return

            for callback in self.callbacks:
                callback(self._application)

            self.updateEvent.set()
            time.sleep(self.period)

    def live_plot_notebook(self, n_iter=None) -> None:
        """Useful in the remote case only."""
        cnt = 0
        while True:
            cnt += 1
            if n_iter is not None and cnt > n_iter:
                return
            if self.stopped():
                return

            sparkmon.plot_notebook(self._application)

            self.updateEvent.clear()
            self.updateEvent.wait()
