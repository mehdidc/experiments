#!/usr/bin/env python

import xmlrpclib

from db import Model, JobModel, CommandModel
from bson.objectid import ObjectId

import json
import Pyro4

from util import datetime_from_str, datetime_to_str
import pymongo
Pyro4.config.SERIALIZER = 'pickle'
Pyro4.config.SERIALIZERS_ACCEPTED.add('pickle')

def show_job_model_short(job_model):
    print "ID:%s Command:'%s' Start:%s, Stop:%s" % (job_model._id, 
        " ".join(job_model.command_model.args), datetime_to_str(job_model.datetime_from),
        datetime_to_str(job_model.datetime_to))

def show_job_model_long(job_model):
    print "ID:%s" % (job_model._id)
    print "Stdout:"
    print job_model.output_data["stdout"]
    print "inputs : %s" % (job_model.input_data.keys(),)
    print "outputs : %s" % (job_model.output_data.keys(),)

def show_job_models(job_models):
    print "%d job(s)..." % (len(job_models))
    for job_model in job_models:
        show_job_model_short(job_model) 

def find_jobs_params(database, state, categories, input_files_contains, output_file_contains, extra):
    params = {"state": state}
    if categories is not None:
        params["command_model"] = {"categories": {"$in": categories}}
    
    for file_contains, type_file in (input_files_contains,"input"), (output_file_contains, "output"):

        param_name = type_file + "_data"
        if type(file_contains) == str:
            params[param_name] = {"*": file_contains}
        elif type(file_contains) == list:
            params[param_name] = {"*": {"$in":file_contains } }
        elif type(file_contains) == dict:
            params[param_name] = {}
            for filename, contains in file_contains.items():
                if type(contains) == list:
                    contains = {"$in": contains}
                params[param_name][filename] = contains
    if extra is not None:
        params.update(json.loads(extra))
    return params


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('action', help="curjobs | pastjobs | commands | newcommand | new-job | dropalljobs | jobdetails | dropjob | jobdata")
    parser.add_argument('--command-id', help="newjob | dropcommand", required=False)
    parser.add_argument('--job-id', help="jobdetails | dropjob | jobdata ", required=False)
    parser.add_argument('--input-files', help="newcommand ", required=False, nargs="*")
    parser.add_argument('--output-files', help="newcommand", required=False, nargs="*")
    parser.add_argument('--args', help="newcommand ", required=False)
    parser.add_argument('--cwd', help="newcommand", required=False)
    parser.add_argument('--categories', help="newcommand | commands | curjobs | pastjobs", required=False, nargs="*")
    parser.add_argument('--extra', help="commands | curjobs | pastjobs", required=False)
    parser.add_argument('--date-from', help="curjobs | pastjobs", required=False)
    parser.add_argument('--date-to', help="curjobs | pastjobs", required=False)
    parser.add_argument('--date', help="curjobs | pastjobs", required=False)
    parser.add_argument('--data-name', help='jobdata', required=False) 
       
    args = parser.parse_args()
    server = "localhost:50490"
    database = Pyro4.Proxy("PYRO:database@%s" % (server,))
    interface = Pyro4.Proxy("PYRO:interface@%s" % (server,))
    
    params = {} 
    if args.date is not None:
        date = datetime_from_str(args.date)
        params["datetime_from"] = {"$gte": date}
        params["datetime_to"] = {"$lte": date}
    if args.date_from is not None:
        date_from = datetime_from_str(args.date_from)
        params["datetime_from"] = {"$gte": date_from}
    if args.date_to is not None:
        date_to = datetime_from_str(args.date_to)
        params["datetime_to"] = {"$lte": date_to}


    if args.action == "curjobs":
        input_file_contains, output_file_contains = None, None
        params.update(find_jobs_params(database, JobModel.RUNNING, args.categories, input_file_contains, output_file_contains, args.extra))
        job_models = database.find(JobModel, params, sort=[ ("datetime_from", 1), ("datetime_to", 2) ])
        show_job_models(job_models)
    elif args.action == "pastjobs":
        input_file_contains, output_file_contains = None, None
        params.update(find_jobs_params(database, JobModel.STOPPED, args.categories, input_file_contains, output_file_contains, args.extra))
        job_models = database.find(JobModel, params, sort=[ ("datetime_from", 1), ("datetime_to", 2) ])
        show_job_models(job_models)
    elif args.action == "commands":
        params = {}
        if args.categories is not None:
            params["categories"] = {"$in": args.categories}
        if args.extra is not None:
            params.update(json.loads(args.extra))
        command_models = database.find(CommandModel, params)
        for command_model in command_models:
            print str(command_model)
    elif args.action == "newcommand":
        if args.cwd is None: args.cwd = "."
        command_model = CommandModel(args.args.split(), 
            input_files=args.input_files, output_files=args.output_files, cwd=args.cwd, categories=args.categories)
        database.insert(command_model)
        print command_model
    elif args.action == "newjob":
        command_model_id = ObjectId(args.command_id)
        command_model = database.find_one(CommandModel, command_model_id)
        job_model = JobModel(command_model)
        job_model._id = database.insert(job_model)
        interface.new_process(job_model)
    elif args.action == "dropalljobs":
        database.drop(JobModel)
    elif args.action == "dropallcommands":
        database.drop(CommandModel)
    elif args.action == "dropjob":
        job_id = ObjectId(args.job_id)
        database.remove(JobModel, job_id)
    elif args.action == "dropcommand":
        command_id = ObjectId(args.command_id)
        database.remove(CommandModel, command_id) 
    elif args.action == "jobdetails":
        job_model = database.find_one(JobModel, ObjectId(args.job_id))
        show_job_model_long(job_model)
    elif args.action == "jobstop":
        interface.kill_process(ObjectId(args.job_id))
    elif args.action == "jobdata":
        job_model = database.find_one(JobModel, ObjectId(args.job_id))
        if args.data_name in job_model.input_data:
            print job_model.input_data[args.data_name]
        elif args.data_name in job_model.output_data:
            print job_model.output_data[args.data_name]
       
