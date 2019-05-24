'''
Batch Example
=============
An example of building a pipeline with batched tasks.
'''

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
from nesta.production.luigihacks import autobatch
from nesta.production.orms.orm_utils import setup_es

from eurito_daps.packages.utils import silo
import luigi
import datetime
import json
import time
import os
import logging

class SomeBatchTask(autobatch.AutoBatchTask):
    '''A set of batched tasks which increments the age of the muppets by 1 year.
    Args:
        date (datetime): Date used to label the outputs
        batchable (str): Path to the directory containing the run.py batchable
        job_def (str): Name of the AWS job definition
        job_name (str): Name given to this AWS batch job
        job_queue (str): AWS batch queue
        region_name (str): AWS region from which to batch
        poll_time (int): Time between querying the AWS batch job status
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    age_increment = luigi.IntParameter()
    reindex = luigi.BoolParameter(default=False)

    def output(self):
        '''Points to the output database engine'''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "ExampleBatchTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def prepare(self):
        '''Prepare the batch job parameters'''
        # Create the index + mapping if required
        es_mode = "dev" if self.test else "prod"
        es, es_config = setup_es(es_mode=es_mode, 
                                 test_mode=self.test, 
                                 reindex_mode=self.reindex,
                                 dataset='example',
                                 aliases='example')

        # Open the input file
        data = silo.get("example")
        job_params = []
        for i, row in enumerate(data):
            params = {"aws_auth_region": es_config["region"],
                      "outinfo": es_config["host"],
                      "dataset" : "example",
                      "done": False,
                      "age_increment": self.age_increment,
                      "start_index":i,
                      "end_index":i+1,
                      "out_type": es_config["type"],
                      "out_port": es_config["port"],
                      "out_index":es_config["index"],
                      "entity_type":"muppet"}
            job_params.append(params)
        return job_params

    def combine(self, job_params):
        '''Combine the outputs from the batch jobs'''
        self.output().touch()


class RootTask(luigi.WrapperTask):
    '''The root task, which adds the surname 'Muppet'
    to the names of the muppets.
    Args:
        date (datetime): Date used to label the outputs
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    age_increment = luigi.IntParameter()
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Get the output from the batchtask'''
        logging.getLogger().setLevel(logging.INFO)
        return SomeBatchTask(date=self.date,
                             age_increment=self.age_increment,
                             test=not self.production,
                             batchable=find_filepath_from_pathstub("batchables/example/some_task"),
                             env_files=[find_filepath_from_pathstub("eurito_daps/eurito_daps/")],
                             job_def="py36_amzn1_image",
                             job_name="batch-example-%s" % self.date,
                             job_queue="HighPriority",
                             region_name="eu-west-1",
                             poll_time=60)
