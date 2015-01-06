from multiprocessing import Process, Queue, Pipe, Value
from subprocess import Popen, PIPE
import sys, os
import signal

from db import JobModel

from util import is_process_running
from datetime import datetime

from Queue import Empty as EmptyQueue

class ProcessManager(object):
    # job_model will be updated by ProcessManager
    def __init__(self, job_model):
        self.job_model = job_model
        self.output_queue = Queue()

        env = {}
        env.update(os.environ)
        env["jobid"] = str(self.job_model._id)

        self.outer_process = Process(target=run_command, 
            args=[self.job_model.command_model.args, 
                  self.output_queue, self.job_model.command_model.cwd, env])
        self.inner_process_pid = None

        self.job_model.state = JobModel.NOT_YET_STARTED

    def start_and_update_state(self):
        self.__start()
        self.job_model.state = JobModel.RUNNING
        assert (self.job_model.datetime_from, self.job_model.datetime_to) == (None, None)
        self.job_model.datetime_from, self.job_model.datetime_to = (datetime.now(), None)

    def __start(self):
        assert self.inner_process_pid == None
        self.job_model.store_input_data_from_command_model()
        self.outer_process.start()
        self.inner_process_pid = self.output_queue.get()

    def kill_and_update_state(self):
        self.__kill()
        self.job_model.state = JobModel.STOPPED
        self.__stopped()
        started_datetime, stopped_datetime = self.job_model.datetime_from, self.job_model.datetime_to
        assert started_datetime is not None and stopped_datetime is None
        self.job_model.datetime_from, self.job_model.datetime_to = started_datetime, datetime.now()


    def __kill(self):
        assert self.inner_process_pid is not None
        self.__kill_inner_process()
        self.outer_process.terminate()

    def __kill_inner_process(self):
        try:
            os.kill(self.inner_process_pid, signal.SIGKILL)
        except OSError:
            print "could not kill %d" % (self.inner_process_pid,)

    def is_alive(self):
        if self.outer_process.is_alive():
            assert self.inner_process_pid is not None
            return self.__is_inner_process_alive()
        else:
            return False

    # If the inner process is alive and the outer process is not alive, kill the inner process
    def check_if_alive_and_update_state(self):
        if self.job_model.state != JobModel.RUNNING:
            return
        #assert self.job_model.state ==  JobModel.RUNNING
        if not self.is_alive():
            if self.__is_inner_process_alive():
                self.__kill_inner_process()
            self.job_model.state = JobModel.STOPPED
            self.__stopped()
            started_datetime, stopped_datetime = self.job_model.datetime_from, self.job_model.datetime_to
            assert started_datetime is not None and stopped_datetime is None
            self.job_model.datetime_from, self.job_model.datetime_to = started_datetime, datetime.now()

    def __is_inner_process_alive(self):
        return is_process_running(self.inner_process_pid)

    # get a list of available stdout output as a list of strings (lines)
    # this procedure is not blocking, it returns only the available lines
    def get_available_output(self):
        output = []
        while True:
            try:
                data =  self.output_queue.get(block=False)
            except EmptyQueue:
                break
            else:
                if data is not Finish:
                    output.append(data)
                else:
                    break
        return output

    def add_available_output_to_job_model_and_get_nblines(self):
        output = self.get_available_output()
        self.job_model.output_data["stdout"] += "".join(output)
        return len(output)

    # event when the start turn on stopped
    def __stopped(self):
        self.job_model.store_output_data_from_command_model()


Finish = None
def run_command(args, output_queue, cwd=".", env=None):
    handle = Popen(args, cwd=cwd, stdout=PIPE, universal_newlines=True, env=env)
    output_queue.put(handle.pid)
    while True:
        line = handle.stdout.readline()
        if line:
            output_queue.put(line)
        else:
            break
    output_queue.put(Finish)
    print "finish"


class ProcessManagerDatabaseSync(object):

    def __init__(self, process_manager, database):
        self.process_manager = process_manager
        self.database = database

    def sync(self):
        self.database.save(self.process_manager.job_model)


# manage multiple ProcessManagers and update  sync with the database
class MultipleProcessesManager(object):

    def __init__(self):
        self.processes = []

    def add_process(self, process):
        self.processes.append(process)
    
    def kill_process_with_job_id(self, job_id, database):
        found = False
        for process in self.processes:
            if process.job_model._id == job_id:
                process.kill_and_update_state()
                ProcessManagerDatabaseSync(process, database).sync()
                found = True
                break
        if not found:
            job_model = database.find_one(JobModel, job_id)
            if job_model:
                job_model.state = JobModel.STOPPED
                database.save(job_model)

    def start_new_processes_and_update_states(self, database):
        for process in self.processes:
            if process.job_model.state == JobModel.NOT_YET_STARTED:
                process.start_and_update_state()
                assert process.job_model.state == JobModel.RUNNING
                ProcessManagerDatabaseSync(process, database).sync()

    def processes_update_states(self, database):
        for process in self.processes:
            old_state = process.job_model.state
            process.check_if_alive_and_update_state()
            # update only if state has changed
            if old_state != process.job_model.state:
                ProcessManagerDatabaseSync(process, database).sync()

    def add_available_output_to_job_model(self, database):
        for process in self.processes:
            nblines = process.add_available_output_to_job_model_and_get_nblines()
            # update only if there are new lines
            if nblines > 0:
                ProcessManagerDatabaseSync(process, database).sync()

    # must be called AFTER processes_update_states and add_available_output_to_job_model
    def delete_finished_processes(self):
        self.processes = [process for process in self.processes
                          if process.job_model.state != JobModel.STOPPED]

if __name__ == "__main__":
    from db import CommandModel

    command_model = CommandModel(["python", "-u", "test.py"], input_files=["test.py"], output_files=["util.py"])
    job_model = JobModel(command_model)


    p = ProcessManager(job_model)
    p.start_and_update_state()

    while True:
        data =  p.output_queue.get()
        if data is not Finish:
            print data
        else:
            break
    p.check_if_alive_and_update_state()
    print job_model.output_data
