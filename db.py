# -*- coding: utf-8 -*-

from util import (default_value, get_formatted_list, 
    get_formatted_element, DatetimeRange)
import os
import cPickle as pickle
import pymongo
from copy import deepcopy

from bson.objectid import ObjectId

class Database(object):
    def __init__(self, db):
        assert isinstance(db, pymongo.database.Database)
        self.db = db

    # proxy to mongo db find
    def find(self, model_cls, *args, **kwargs):
        assert issubclass(model_cls, Model)
        if "sort" in kwargs:
            sort_ = kwargs["sort"]
            del kwargs["sort"]
        else:
            sort_ = []
        models = self.db[model_cls.__name__].find(*args, **kwargs)
        if len(sort_):
            models = models.sort(sort_)
        g = (model_cls.from_db(model_cls, data) 
                for data in models) 
        return list(g)
    # proxy to mongo db find_one
    def find_one(self, model_cls, id_):
        assert issubclass(model_cls, Model)

        result = self.db[model_cls.__name__].find_one(ObjectId(id_))
        if result is None:
            return None
        return model_cls.from_db(model_cls, result)

    # proxy to mongo db insert
    def insert(self, object):
        assert isinstance(object, Model)
        return self.db[object.__class__.__name__].insert(object.to_db())

    # proxy to mongo db save (= update of one object)
    def save(self, object):
        assert object._id is not None
        result = object.to_db()
        return self.db[object.__class__.__name__].save(result)

    # proxy to mongo db drop = Delete all instances
    def drop(self, model_cls, *args, **kwargs):
        assert issubclass(model_cls, Model)
        return self.db[model_cls.__name__].drop(*args, **kwargs)
    #proxy to mongo db remove = remove some elements
    def remove(self, model_cls, *args, **kwargs):
        assert issubclass(model_cls, Model)
        return self.db[model_cls.__name__].remove(*args, **kwargs)
    
    

class Model(object):

    def __init__(self):
        self._id = None

    def to_db(self):
        d = deepcopy(self.__dict__)
        if "_id" in d and d["_id"] is None:
            del d["_id"]
        return d

    @staticmethod 
    def from_db(cls_model, data):
        inst = cls_model.__new__(cls_model)
        inst.__dict__.update(data)
        return inst

class CommandModel(Model):

    def __init__(self, args, input_files=None, output_files=None, cwd=".", categories=None):
        Model.__init__(self)
        self.args = args
        self.input_files = default_value(input_files, [])
        self.output_files = default_value(output_files, [])
        self.categories = default_value(categories, [])
        self.cwd = cwd

    def __str__(self):
        return str(self.to_db())

class CommandTemplateModel(CommandModel):

    def to_command_model(self, **values):
        args = get_formatted_list(self.args, **values)
        input_files = get_formatted_list(self.input_files, **values)
        output_files = get_formatted_list(self.output_files, **values)
        cwd = get_formatted_element(self.cwd, **values)
        return CommandModel(args, input_files, output_files, cwd)


class JobModel(Model):
    
    NOT_YET_STARTED, RUNNING, STOPPED = range(3)
    STATES = (NOT_YET_STARTED, RUNNING, STOPPED)

    def __init__(self, command_model, 
                       execution_datetime_range=None, state=NOT_YET_STARTED, 
                       input_data=None, output_data=None):
        Model.__init__(self)
        self.command_model = command_model
        #self.execution_range_datetime = default_value(execution_datetime_range, (None, None))
        if execution_datetime_range is None:
            execution_datetime_range = (None, None)
        self.datetime_from = default_value(execution_datetime_range[0], None)
        self.datetime_to = default_value(execution_datetime_range[1], None)
        self.state = state
        self.input_data = default_value(input_data, {})
        self.output_data = default_value(input_data, {})
        self.output_data["stdout"] = ""

    def store_input_data_from_command_model(self):
        self.input_data.update(
            self.get_name_content_mapping_from_filenames(map(self.format_str, self.command_model.input_files))
        )

    def store_output_data_from_command_model(self):
        self.output_data.update(
            self.get_name_content_mapping_from_filenames(map(self.format_str, self.command_model.output_files))
        )

    def get_name_content_mapping_from_filenames(self, filenames):
        mapping = {}
        for filename in filenames:
            abs_filename_path = os.path.join(self.command_model.cwd, filename)
            filename = filename.replace(".", "__") # mongo does not support "."
            try:
                mapping[filename] = open(abs_filename_path, "r").read()
            except:
                pass
        return mapping

    def to_db(self):
        dict = Model.to_db(self)
        dict['command_model'] = self.command_model.to_db()
        return dict

    @staticmethod
    def from_db(cls_model, data):
        inst = Model.from_db(cls_model, data)
        inst.command_model = Model.from_db(CommandModel, inst.command_model)
        return inst

    def __str__(self):
        return str(self.to_db())

    def format_str(self, s):
        return s % {"jobid": str(self._id)}
if __name__ == "__main__":
    tpl = CommandTemplateModel(["ls", "-l"])
    command_model = tpl.to_command_model(a=1, b=2) 
    job = JobModel(command_model)

    client = pymongo.MongoClient()
    mongo_db = client["Test"]

    db = Database(mongo_db)
    mongo_db.JobModel.drop()
    id = db.insert(job)
    one = db.find_one(JobModel, id)
    print one
    db.save(one)
    client.close()
