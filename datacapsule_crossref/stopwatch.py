import sys
import time

try:
  perf_counter = time.perf_counter
except AttributeError:
  # as per original timeit source (before perf_counter)
  if sys.platform == "win32":
    # On Windows, the best timer is time.clock()
    perf_counter = time.clock
  else:
    # On most other platforms the best timer is time.time()
    perf_counter = time.time

class StopWatch(object):
  def __init__(self):
    self.start = perf_counter()

  def get_elapsed_seconds(self, reset=False):
    end = perf_counter()
    elapsed = end - self.start
    if reset:
      self.start = end
    return elapsed

class StopWatchRecorder(object):
  def __init__(self):
    self.stop_watch = StopWatch()
    self.recorded_timings = []
    self.started = None

  def stop(self):
    self.start(None)

  def start(self, name):
    elapsed = self.stop_watch.get_elapsed_seconds(reset=True)
    if self.started:
      self.recorded_timings.append((self.started, elapsed))
    self.started = name

  def __str__(self):
    total = ('total', sum(elapsed for _, elapsed in self.recorded_timings))
    return ', '.join(
      '%s: %.6fs' % (name, elapsed)
      for name, elapsed in self.recorded_timings + [total]
    )
