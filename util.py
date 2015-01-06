import datetime
dt_format = "%d/%m/%y %H:%M"
def datetime_from_str(s):
    dt = datetime.datetime.strptime(s, dt_format)
    return dt

def datetime_to_str(dt):
    if dt is None:
        return "None"
    return dt.strftime(dt_format)

def default_value(val, default_value_):
	if val is None:
		return default_value_
	else:
		return val

def get_formatted_list(iterable, **values):
	return [element % values for element in iterable]

def get_formatted_element(element, **values):
	return element % values

class DatetimeRange(object):

	def __init__(self, start=None, end=None):
		self.start = start
		self.end = end

def get_filtered_dict(keys_filter):
	return dict((key, value) for key, value in self.__dict__.items() if keys_filter(key))


from StringIO import StringIO
class QueueIO(StringIO):
    def __init__(self, queue, *args, **kwargs):
        StringIO.__init__(self, *args, **kwargs)
        self.queue = queue
    def flush(self):
        self.queue.put(self.getvalue())

import os
def is_process_running(pid):
	#http://stackoverflow.com/questions/7647167/check-if-a-process-is-running-in-python-in-linux-unix
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False
