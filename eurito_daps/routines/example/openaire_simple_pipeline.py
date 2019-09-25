'''
Simple Example
==============
An example of building a pipeline with just a wrapper task and a regular task.
'''

from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config

from eurito_daps.packages.utils import openaire_utils
from eurito_daps.packages.utils import globals
from eurito_daps.core.orms.openaire_orm import Base

from nesta.core.orms.orm_utils import get_mysql_engine

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
        output_type (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        production (bool): test mode or production mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    output_type = luigi.Parameter()
    production = luigi.BoolParameter(default=False)

    def requires(self):
        '''Call the task to run before this in the pipeline.'''

        logging.getLogger().setLevel(logging.INFO)
        return CollectOpenAireTask(date=self.date,
                          output_type=self.output_type,
                          test=not self.production)


class CollectOpenAireTask(luigi.Task):
    '''Collects OpenAIRE projects and writes them into MySQL database

    Args:
        date (datetime): Date used to label the outputs
        output_type (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        test (bool): run a shorter version of the task if in test mode
    '''
    date = luigi.DateParameter(default=datetime.datetime.today())
    output_type = luigi.Parameter()
    test = luigi.BoolParameter()

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], 'mysqldb')
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Example <dummy>"  # Note, not a real table
        update_id = "SimpleTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        '''Collects records from OpenAIRE API and stores them into the 'dev' or 'production' database
        '''
        base_url = 'http://api.openaire.eu/oai_pmh'

        #engine = create_engine('sqlite:///pythonsqlite.db', echo=False)

        db = 'dev' if self.test else 'production'
        engine = get_mysql_engine("MYSQLDBCONF", 'mysqldb', db)

        Base.metadata.create_all(engine)

        #open DB session
        Session = sessionmaker(bind=engine)
        dbsession = Session()

        #open up requests session
        reqsession = requests.session()
        reqsession.keep_alive = False

        resumption_token = 'First request'

        #requests.get(url, params={'metadataPrefix':'oaf', 'set':output_type})

        #current_url = base_url + '&metadataPrefix=oaf&set=' + self.output_type
        #logging.info("Current URL:" + current_url)

        count = 0

        while resumption_token != 'None':

            logging.info("Retrieving soup from URL...")
            cur_soup = openaire_utils.get_soup_contents(base_url, reqsession, self.output_type, resumption_token)
            logging.info("Soup returned...")
            logging.info(cur_soup)

            titletag = cur_soup.find('title')
            #logging.debug(titletag.text)

            #obtain resumption_token from soup and update the request url
            resumption_token = openaire_utils.get_res_token(cur_soup)
            #current_url = base_url + '&resumption_token=' + resumption_token
            logging.info("resumption_token: ", resumption_token)


            #parse records
            if self.output_type == 'software':
                cur_records = openaire_utils.parse_soft(cur_soup)
            if self.output_type == 'ECProjects':
                cur_records = openaire_utils.parse_proj(cur_soup)

            #write records into database
            openaire_utils.write_records_to_db(cur_records, self.output_type, dbsession)

            count += 1

            #if count > 1 and self.test:
            #    logging.info("Breaking after 2 results in test mode")
            #    break

        #close DB session
        dbsession.close()

        logging.debug('Writing to DB complete')

        # mark as done
        logging.info("Task complete")
        self.output().touch()
