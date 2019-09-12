'''
Simple Example
==============
An example of building a pipeline with just a wrapper task and a regular task.
'''

from nesta.production.luigihacks.mysqldb import MySqlTarget
from nesta.production.luigihacks.misctools import get_config

from eurito_daps.packages.utils import openaire_utils
from eurito_daps.packages.utils import globals
from eurito_daps.production.orms.openaire_orm import Base

from nesta.production.orms.orm_utils import get_mysql_engine

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import luigi
import datetime
import os
import logging
import requests
import time

class RootTask(luigi.WrapperTask):
    '''The root task, which collects the supplied parameters and calls the SimpleTask.

    Args:
        date (datetime): Date used to label the outputs
        outputType (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        production (bool): test mode or production mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    outputType = luigi.Parameter()
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Call the task to run before this in the pipeline.'''

        logging.getLogger().setLevel(logging.INFO)
        return CollectOpenAireTask(date=self.date,
                          outputType=self.outputType,
                          test=not self.production)


class CollectOpenAireTask(luigi.Task):
    '''Collects OpenAIRE projects and writes them into MySQL database

    Args:
        date (datetime): Date used to label the outputs
        outputType (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        test (bool): run a shorter version of the task if in test mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    outputType = luigi.Parameter()
    test = luigi.BoolParameter()

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "SimpleTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        '''Collects records from OpenAIRE API and stores them into the 'dev' or 'production' database
        '''
        base_url = 'http://api.openaire.eu/oai_pmh?verb=ListRecords'

        #engine = create_engine('sqlite:///pythonsqlite.db', echo=False)

        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine("MYSQLDBCONF", 'mysqldb', db)

        Base.metadata.create_all(engine)

        #open DB session
        Session = sessionmaker(bind=engine)
        globals.dbsession = Session()

        #open up requests session
        reqsession = requests.session()
        reqsession.keep_alive = False

        resumption_token = 'Not None'

        current_url = base_url + '&metadataPrefix=oaf&set=' + self.outputType
        logging.info("Current URL:" + current_url)

        count = 0

        while resumption_token != 'None':

            logging.info("Retrieving soup from URL...")
            cur_soup = openaire_utils.get_soup_contents(current_url, reqsession)
            logging.info("Soup returned...")
            logging.info(cur_soup)

            titletag = cur_soup.find('title')
            logging.info(titletag)

            if titletag.text == '503 Service Unavailable' or titletag.text == '502 Proxy Error':
                logging.info("Service unavailable, waiting 10 seconds and trying again")
                time.sleep(10)
                continue

            #obtain resumption_token from soup and update the request url
            resumption_token = openaire_utils.get_res_token(cur_soup)
            current_url = base_url + '&resumption_token=' + resumption_token
            logging.info ("Next URL: " + current_url)

            #parse records
            if self.outputType == 'software':
                cur_records = openaire_utils.parse_software_soup_rt(cur_soup)
            if self.outputType == 'ECProjects':
                cur_records = openaire_utils.parse_projects_soup_rt(cur_soup)

            #write records into database
            openaire_utils.write_records_to_db(cur_records, self.outputType, globals.dbsession)

            count += 1

            #if count > 1 and self.test:
            #    logging.info("Breaking after 2 results in test mode")
            #    break

        #close DB session
        globals.dbsession.close()

        logging.info('Writing to DB complete')

        # mark as done
        logging.info("Task complete")
        self.output().touch()
