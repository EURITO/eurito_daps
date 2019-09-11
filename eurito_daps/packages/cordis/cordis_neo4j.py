from py2neo import Graph, NodeMatcher
from py2neo.data import Node, Relationship
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.schema import ForeignKeyConstraint
from nesta.core.orms.orm_utils import db_session_query, get_mysql_engine
from nesta.core.orms.orm_utils import get_class_by_tablename, object_to_dict
from nesta.core.orms.orm_utils import graph_session
from nesta.core.luigihacks.misctools import get_config
from nesta.core.orms.cordis_orm import Base, Project
import logging


def orm_to_neo4j(db, tx, parent, row, this, rel):
    row = flatten(row)
    if rel is not None:
        fwd_rel, back_rel = build_relationships(db=db, graph=tx.graph,
                                                parent=parent, row=row,
                                                this=this, rel=rel)
        # Both nodes in the relationship were found
        if (fwd_rel, back_rel) != (None, None):
            tx.create(fwd_rel)
            tx.create(back_rel)
    else:
        set_constraints(this, tx.graph.schema)
        tx.create(Node(extract_name(this.__tablename__), **row))


def build_relationships(db, graph, parent, row, this, rel):
    # Case 1) `this` is a node
    if parent is None:
        this_node = Node(extract_name(this.__tablename__), **row)
        rel_props = {}
    # Case 2) `this` is a relationship
    else:
        this_node = retrieve_node(db, graph, parent, row, this)
        rel_props = row
    # Also retrieve the Project node from neo4j
    proj_node = retrieve_node(db, tx.graph, Project, row, this)
    # If either node is not found, give up
    if proj_node is None or this_node is None:
        return None, None
    # Build a forward and backward relationship, wrt the project
    relationship = Relationship(proj_node, rel, this_node, **rel_props)
    back_relationship = Relationship(this_node, 'HAS_PROJECT', proj_node)
    return relationship, back_relationship


def set_constraints(orm, graph_schema):
    entity_name = extract_name(orm.__tablename__)
    (pk,) = inspect(orm).primary_key
    constrs = graph_schema.get_uniqueness_constraints(entity_name)
    assert len(constrs) <= 1
    if len(constrs) == 0:
        logging.info('Creating constraint on '
                     f'{entity_name}.{pk.name}')
        graph_schema.create_uniqueness_constraint(entity_name,
                                                  pk.name)
    else:
        assert len(constrs[0]) == 1


def prepare_base_entities(table):
    """Returns the objects required to generate a graph
    representation of the ORM.

    Args:
        table (sqlalchemy.sql.Table): SQL alchemy table object from which
                                      to extract an graph representation.
    Returns:
        {this, parent, rel} ({Base, Base, str}): Two ORMs and a string
                                                 describing their relationship
    """
    fks = [fk for c in table.columns
           if c.foreign_keys
           for fk in c.foreign_keys]
    this = get_class_by_tablename(Base, table.name)
    parent, rel = None, None
    if len(fks) == 1:
        rel = f'HAS_{entity_name.upper()}'
    elif len(fks) == 2:
        _tablename = table_from_fk(fks)
        rel = f'HAS_{extract_name(_tablename).upper()}'
        parent = get_class_by_tablename(Base, _tablename)
    return this, parent, rel


def flatten(row):
    row = object_to_dict(row, shallow=True)
    return {k: _flatten(v) for k, v in row.items()}


def _flatten(data):
    if type(data) is dict:
        return flatten_dict(data)
    elif type(data) is not list:
        return data
    types = set(type(row) for row in data)
    if len(types) > 1:
        raise TypeError(f'Mixed types ({types}) are not accepted')
    try:
        if next(iter(types)) is dict:
            return flatten_json(data)
    except StopIteration:
        pass
    return data


def flatten_dict(row, keys=[('title',),
                            ('street', 'city', 'postalCode')]):
    flat = []
    for ks in keys:
        if not any(k in row for k in ks):
            continue
        flat = '\n'.join(row[k] for k in ks
                         if k in row)
        break
    return flat


def flatten_json(data, keys=[('title',),
                             ('street', 'city', 'postal_code')]):
    flat_data = [flatten_dict(row, keys) for row in data]
    assert len(flat_data) == len(data)
    return flat_data


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
    return flatten(_row)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    limit = 10

    engine = get_mysql_engine('MYSQLDB', 'nesta', 'production')
    conf = get_config('neo4j.config', 'neo4j')
    gkwargs = dict(host=conf['host'], secure=True,
                   auth=(conf['user'], conf['password']))

    with graph_session(**gkwargs) as tx:
        logging.info('Dropping all previous data')
        tx.graph.delete_all()
        for constraint in tx.run('CALL db.constraints'):
            logging.info(f'Dropping constraint {constraint[0]}')
            tx.run(f'DROP {constraint[0]}')

    for tablename, table in Base.metadata.tables.items():
        entity_name = extract_name(tablename)
        logging.info(f'\tProcessing {entity_name}')
        this, parent, rel = prepare_base_entities(table)
        with graph_session(**gkwargs) as tx:
            for db, row in db_session_query(query=this, engine=engine,
                                            limit=limit):
                orm_to_neo4j(db=db, tx=tx, parent=parent,
                             row=row, this=this, rel=rel)
