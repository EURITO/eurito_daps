'''
OpenAIRE to Neo4j
=================

Pipe data directly from the OpenAIRE API to Neo4j by matching to Cordis projects
already in Neo4j.
'''

from nesta.core.luigihacks.mysqldb import MySqlTarget
from nesta.core.luigihacks.misctools import get_config

from eurito_daps.packages.utils import openaire_utils

from nesta.core.orms.orm_utils import get_mysql_engine
from nesta.core.orms.orm_utils import graph_session

from py2neo.data import Relationship

import luigi
import datetime
import os
import logging
import requests


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
        return OpenAireToNeo4jTask(date=self.date,
                          output_type=self.output_type,
                          test=not self.production)

class OpenAireToNeo4jTask(luigi.Task):
    '''Takes OpenAIRE entities from MySQL database and writes them into Neo4j database

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
        update_id = "OpenAireToNeo4jTask_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):

        def get_page_records (soup):
            all_results = list()

            return all_results

        # Get connection settings
        engine = get_mysql_engine('MYSQLDB', 'mysqldb',
                                  'dev' if self.test else 'production')
        conf = get_config('neo4j.config', 'neo4j')
        gkwargs = dict(host=conf['host'], secure=True,
                       auth=(conf['user'], conf['password']))

        # open up requests session
        reqsession = requests.session()
        reqsession.keep_alive = False
        base_url = 'http://api.openaire.eu/search/'
        count = 0

        # for each project in Neo4j
        with graph_session(**gkwargs) as tx:
            graph = tx.graph

            neo_projects = graph.nodes.match("Project")

            total_projects = len(neo_projects)

            sum = 0

            for index, neo_project in enumerate(neo_projects):
                if index % 1000 == 0:
                    logging.info("Checking project %d out of %d" % (index, total_projects) )

                souplist = openaire_utils.get_project_soups(base_url, reqsession,
                                                            self.output_type, neo_project.identity)
                # get all results, not just from one page
                results = openaire_utils.get_results_from_soups(souplist)

                sum = sum + len(results)
                if len(results) > 0:
                    logging.info("Found %d related records" % len(results))

                for result in results:
                    title = result.find("title", recursive=False)
                    pid = result.find("pid", recursive=False)

                    record_obj = dict()
                    record_obj['title'] = title.text
                    record_obj['pid'] = pid.text

                    # create record object in Neo4j and return it
                    created_node = openaire_utils.write_record_to_neo(record_obj, self.output_type, graph)

                    # create relationship between neo_project and record_obj
                    relationship_type = "HAS_" + self.output_type.upper()

                    project_has_node = Relationship(neo_project, relationship_type, created_node)
                    graph.create(project_has_node)

                    node_has_project = Relationship(created_node, "HAS_PROJECT", neo_project)
                    graph.create(node_has_project)

                if index > 60000 and self.test:
                    logging.info("Breaking after %d results in test mode" % index)
                    break

        # mark as done
        logging.info("Task complete")
        self.output().touch()
