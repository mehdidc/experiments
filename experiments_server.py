#!/usr/bin/env python

import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer
from threading import Thread, Event

from process import MultipleProcessesManager, ProcessManager
import pymongo

from db import Database, CommandModel
import time



class Interface(object):
    def __init__(self, database, multiple_processes_manager):
        self.database = database
        self.multiple_processes_manager = multiple_processes_manager

    def new_process(self, job_model):
        assert job_model._id is not None
        process = ProcessManager(job_model)
        self.multiple_processes_manager.add_process(process)
        self.multiple_processes_manager.start_new_processes_and_update_states(self.database)
    
    def kill_process(self, job_id):
        self.multiple_processes_manager.kill_process_with_job_id(job_id, self.database)    


Stop = False
def multiple_processes_manager_poll(multiple_processes_manager, database, stop_event):
    while not stop_event.is_set():
        time.sleep(1)
        multiple_processes_manager.processes_update_states(database)
        multiple_processes_manager.add_available_output_to_job_model(database)
        multiple_processes_manager.delete_finished_processes()


import Pyro4
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')
if __name__ == "__main__":

    client = pymongo.MongoClient()
    mongo_db = client["Test"]
    database = Database(mongo_db)
    multiple_processes_manager = MultipleProcessesManager()
    interface = Interface(database, multiple_processes_manager)

    print "Staring MPM Poll thread..."

    stop_event = Event()
    multiple_processes_manager_poll_thread = (
        Thread(target=multiple_processes_manager_poll, 
            args=(multiple_processes_manager, database, stop_event),
            )
    )
    multiple_processes_manager_poll_thread.daemon = True
    multiple_processes_manager_poll_thread.start()
    try:
        Pyro4.Daemon.serveSimple(
                {
                    database: "database",
                    interface: "interface"
                },
                ns = False, port=50490)
    except KeyboardInterrupt:
        stop_event.set()
