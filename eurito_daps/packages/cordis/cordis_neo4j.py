from py2neo import Graph, NodeMatcher
from py2neo.data import Node, Relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import ForeignKeyConstraint
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import get_class_by_tablename, object_to_dict
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.misctools import get_config
from nesta.core.orms.cordis_orm import Base, Project
import logging


def retrieve_node(db, graph, parent, row, this):
    row = get_row(db, parent, row, this)
    (pk,) = inspect(parent).primary_key
    matcher = NodeMatcher(graph)
    return matcher.match(extract_name(parent.__tablename__),
                      **{pk.name: row[pk.name]}).first()


def extract_name(tablename):
    return tablename.replace('cordis_', '')[:-1].title()


def table_from_fk(fks):
    return [fk.column.table.name for fk in fks
            if fk.column.table.name != 'cordis_projects'][0]


def get_row(session, _class, row, this):
    (pk,) = inspect(_class).primary_key
    (this_pk,) = [c for c in this.__table__.columns
                  for fk in c.foreign_keys
                  if fk.column.table.name == _class.__tablename__]
    this_value = row[this_pk.name]
    _row = session.query(_class).filter(pk == this_value).first()
    _row = object_to_dict(_row, shallow=True)
    return {k: v for k, v in _row.items()
            if type(v) not in (dict, list)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    limit = None
    
    engine = get_mysql_engine('MYSQLDB', 'nesta', 'dev')
    conf = get_config('neo4j.config', 'neo4j')
    gkwargs = dict(host=conf['host'], secure=True,
                   auth=(conf['user'], conf['password']))

    with graph_session(**gkwargs) as tx:
        logging.info('Dropping all...')
        tx.graph.delete_all()
        for constraint in tx.run('CALL db.constraints'):
            logging.info(f'Dropping constraint {constraint[0]}')
            tx.run(f'DROP {constraint[0]}')

    for tablename, table in Base.metadata.tables.items():
        entity_name = extract_name(tablename)
        logging.info(f'\tProcessing {entity_name}')
        fks = [fk for c in table.columns
               if c.foreign_keys
               for fk in c.foreign_keys]
        this = get_class_by_tablename(Base, tablename)
        parent, rel = None, None
        if len(fks) == 1:
            rel = f'HAS_{entity_name.upper()}'
        elif len(fks) == 2:
            _tablename = table_from_fk(fks)
            rel = f'HAS_{extract_name(_tablename).upper()}'
            parent = get_class_by_tablename(Base, _tablename)

        with graph_session(**gkwargs) as tx, db_session(engine) as db:
            for row in db.query(this).limit(limit):
                row = object_to_dict(row, shallow=True)
                row = {k: v for k, v in row.items()
                       if type(v) not in (dict, list)}
                obj = Node(entity_name, **row)
                if rel is not None:
                    rel_props = {}
                    if parent is not None:
                        obj = retrieve_node(db, tx.graph, parent,
                                            row, this)
                        rel_props = row
                    proj_node = retrieve_node(db, tx.graph, Project,
                                              row, this)
                    if proj_node is None or obj is None:
                        continue
                    obj = Relationship(proj_node, rel, obj, **rel_props)
                else:
                    s = tx.graph.schema
                    (pk,) = inspect(this).primary_key
                    constrs = s.get_uniqueness_constraints(entity_name)
                    assert len(constrs) <= 1
                    if len(constrs) == 0:
                        logging.info('\t\t\tCreating constraint on '
                                     f'{entity_name}.{pk.name}')
                        s.create_uniqueness_constraint(entity_name,
                                                       pk.name)
                    else:
                        assert len(constrs[0]) == 1
                tx.create(obj)
