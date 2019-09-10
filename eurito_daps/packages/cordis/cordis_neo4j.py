from py2neo import Graph
from py2neo.data import Node, Relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import ForeignKeyConstraint
from nesta.core.orms.orm_utils import db_session, get_mysql_engine
from nesta.core.orms.orm_utils import get_class_by_tablename, object_to_dict
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.misctools import get_config
from nesta.core.orms.cordis_orm import Base, Project


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
    engine = get_mysql_engine('MYSQLDB', 'nesta', 'dev')
    conf = get_config('neo4j.config', 'neo4j')
    gkwargs = dict(host=conf['host'], secure=True,
                   auth=(conf['user'], conf['password']))

    for tablename, table in Base.metadata.tables.items():
        entity_name = extract_name(tablename)
        fks = [fk for c in table.columns
               if c.foreign_keys
               for fk in c.foreign_keys]
        this, parent, rel = get_class_by_tablename(Base, tablename), None, None
        if len(fks) == 1:
            rel = f'HAS_{entity_name.upper()}'
        elif len(fks) == 2:
            _tablename = table_from_fk(fks)
            rel = f'HAS_{extract_name(_tablename).upper()}'
            parent = get_class_by_tablename(Base, _tablename)

        with graph_session(**gkwargs) as graph, db_session(engine) as db:
            for row in db.query(this).limit(10):
                row = object_to_dict(row, shallow=True)
                row = {k: v for k, v in row.items()
                       if type(v) not in (dict, list)}
                obj = Node(entity_name, **row)
                if rel is not None:
                    rel_props = {}
                    if parent is not None:
                        obj = Node(parent.name, **get_row(db, parent, row, this))
                        rel_props = row
                    proj_node = Node('Project', **get_row(db, Project, row, this))
                    obj = Relationship(proj_node, rel, obj, **rel_props)
                print('creating', this, parent, rel, obj)
                graph.create(obj)

