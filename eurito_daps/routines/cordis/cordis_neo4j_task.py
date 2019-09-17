"""
CordisNeo4jTask
===============

Task for piping Cordis data from SQL to Neo4j.
"""

from nesta.core.orms.cordis_orm import Base
from nesta.core.orms.orm_utils import db_session_query, get_mysql_engine
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.misctools import get_config
from eurito_daps.packages.cordis.cordis_neo4j import extract_name, orm_to_neo4j
from eurito_daps.packages.cordis.cordis_neo4j import prepare_base_entities
from nesta.core.luigihacks.mysqldb import MySqlTarget

from datetime import datetime as dt
import luigi
import logging
import os
from py2neo import Graph


class CordisNeo4jTask(luigi.Task):
    test = luigi.BoolParameter(default=True)
    date = luigi.DateParameter(default=dt.now())

    def output(self):
        '''Points to the output database engine where the task is marked as done.
        The luigi_table_updates table exists in test and production databases.
        '''
        db_config = get_config(os.environ["MYSQLDB"], "mysqldb")
        db_config["database"] = 'dev' if self.test else 'production'
        db_config["table"] = "Cordis Neo4j <dummy>"  # Note, not a real table
        update_id = "Cordis2Neo4j_{}".format(self.date)
        return MySqlTarget(update_id=update_id, **db_config)

    def run(self):
        limit = 100 if self.test else None
        flush_freq = 33 if self.test else 5000
 
        # Get connection settings
        engine = get_mysql_engine('MYSQLDB', 'nesta',
                                  'dev' if self.test else 'production')
        conf = get_config('neo4j.config', 'neo4j')
        gkwargs = dict(host=conf['host'], secure=True,
                       auth=(conf['user'], conf['password']))

        # Drop all neo4j data in advance
        # (WARNING: this is a hack in lieu of proper db staging/versioning)
        with graph_session(**gkwargs) as tx:
            logging.info('Dropping all previous data')
            tx.graph.delete_all()
            for constraint in tx.run('CALL db.constraints'):
                logging.info(f'Dropping constraint {constraint[0]}')
                tx.run(f'DROP {constraint[0]}')

        # Iterate over all tables in the ORM
        for tablename, table in Base.metadata.tables.items():
            entity_name = extract_name(tablename)
            logging.info(f'\tProcessing {entity_name}')
            orm, parent_orm, rel_name = prepare_base_entities(table)
            # Insert data to neo4j in one session per table,
            # to enable constraint and relationship lookups
            # after insertion            
            irow = 0
            uninterrupted = False
            while not uninterrupted:
                uninterrupted = True
                with graph_session(**gkwargs) as tx:
                    # Iterate over rows in the database
                    for db, orm_instance in db_session_query(query=orm,
                                                             engine=engine,
                                                             limit=limit, 
                                                             offset=irow):
                        irow += 1
                        if irow == limit:
                            break
                        # Convert the ORM row to a neo4j object, and insert
                        orm_to_neo4j(session=db, transaction=tx,
                                     orm_instance=orm_instance,
                                     parent_orm=parent_orm,
                                     rel_name=rel_name)
                        if (irow % flush_freq) == 0:
                            logging.info(f'\t\tFlushing at row {irow}')
                            uninterrupted = False
                            break
        # Confirm the task is finished
        self.output().touch()


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)

    def requires(self):
        test = not self.production
        set_log_level(test)
        return CordisNeo4jTask(test=test)
