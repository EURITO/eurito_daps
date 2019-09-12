"""
CordisNeo4jTask
===============

Task for piping Cordis data from SQL to Neo4j.
"""

from nesta.core.orms.cordis_orm import Base
from nesta.core.orms.orm_utils import db_session_query, get_mysql_engine
from nesta.core.orms.orm_utils import graph_session
from nesta.core.orms.luigihacks.luigi_logging import set_log_level
from nesta.core.luigihacks.misctools import get_config
import luigi


class CordisNeo4jTask(luigi.Task):
    test = luigi.BoolParameter(default=True)

    def run(self):
        limit = 1000 if self.test else None

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
            with graph_session(**gkwargs) as tx:
                # Iterate over rows in the database
                for db, orm_instance in db_session_query(query=orm,
                                                         engine=engine,
                                                         limit=limit):
                    # Convert the ORM row to a neo4j object, and insert
                    orm_to_neo4j(session=db, transaction=tx,
                                 orm_instance=orm_instance,
                                 parent_orm=parent_orm, rel_name=rel_name)


class RootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=True)

    def requires(self):
        test = not self.production
        set_log_level(test)
        return CordisNeo4jTask(test=test)
