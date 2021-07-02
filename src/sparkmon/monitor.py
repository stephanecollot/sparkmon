"""Monitor thread."""
import sparkmon
import threading
import time
import random
import urllib

class SparkMon(threading.Thread):
    """Class to manage a socket in a background thread to talk to the scala listener."""

    def __init__(self, application: sparkmon.Application, period=20, callbacks=[]):
        """Constructor, initializes base class Thread."""
        threading.Thread.__init__(self)
        self._stop = threading.Event()
        self.cnt = 0
        self._application = application
        self.period = period
        self.callbacks = callbacks
        self.updateEvent = threading.Event()

    def stop(self):
        """To stop the thread."""
        self._stop.set()

    def stopped(self):
        """Overrides Thread method."""
        return self._stop.isSet()

    def run(self):
        """Overrides Thread method."""
        while(True):
            if self.stopped():
                return

            try:
                self._application.log_executors_info()
                self.cnt += 1
            except urllib.error.URLError as ex:
                if self.cnt > 1:
                    print(f"Spark application not available anymore. Exception: {ex}")
                    self.stop()
                    return

            for callback in self.callbacks:
                callback(self._application)

            self.updateEvent.set()
            time.sleep(self.period)

    def live_plot_notebook(self):
        """Useful in the remote case only"""
        while(True):
            if self.stopped():
                return

            sparkmon.plot_notebook(self._application)

            self.updateEvent.clear()
            self.updateEvent.wait()
