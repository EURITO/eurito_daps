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


def orm_to_neo4j(session, transaction, orm_instance,
                 parent_orm=None, rel_name=None):
    """Pipe a SqlAlchemy ORM instance (a 'row' of data)
    to neo4j, inserting it as a node or relationship, as appropriate.

    Args:
        session (sqlalchemy.Session): SQL DB session.
        transaction (py2neo.Transaction): Neo4j transaction
        orm_instance (sqlalchemy.Base): Instance of a SqlAlchemy ORM
        parent_orm (sqlalchemy.Base): Parent ORM to build relationship to
        rel_name (str): Name of the relationship to be added to Neo4j
    """
    graph = transaction.graph
    orm = get_class_by_tablename(Base, orm_instance.__tablename__)
    data_row = flatten(orm_instance)

    # Either neither are specified, or at least one is specified
    assert ((rel_name is None and parent_orm is None) or   # neither
            (rel_name is not None or parent_orm is not None))  # at least one

    # Build a relationship if specified
    if rel_name is not None:
        fwd_rel, back_rel = build_relationships(session=session,
                                                graph=graph,
                                                orm=orm, parent_orm=parent_orm,
                                                data_row=data_row,
                                                rel_name=rel_name)
        # Both nodes in the relationship were found
        if (fwd_rel, back_rel) != (None, None):
            transaction.create(fwd_rel)
            transaction.create(back_rel)
    # Otherwise create a single node, imposing constraints based on SQL FKs
    else:
        set_constraints(orm, graph.schema)
        transaction.create(Node(extract_name(orm.__tablename__), **data_row))


def build_relationships(session, graph, orm, parent_orm, data_row, rel_name):
    # Case 1) `orm` is a node
    if parent_orm is None:
        this_node = Node(extract_name(orm.__tablename__), **data_row)
        rel_props = {}
    # Case 2) `this` is a relationship
    else:
        this_node = retrieve_node(session, graph, orm, parent_orm, data_row)
        rel_props = data_row
    # Also retrieve the Project node from neo4j
    proj_node = retrieve_node(session, graph, orm, Project, data_row)
    # If either node is not found, give up
    if proj_node is None or this_node is None:
        return None, None
    # Build a forward and backward relationship, wrt the project
    relationship = Relationship(proj_node, rel_name, this_node, **rel_props)
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


def retrieve_node(session, graph, orm, parent_orm, data_row):
    row = get_row(session, parent_orm, orm, data_row)
    (pk,) = inspect(parent_orm).primary_key
    matcher = NodeMatcher(graph)
    return matcher.match(extract_name(parent_orm.__tablename__),
                         **{pk.name: row[pk.name]}).first()


def extract_name(tablename):
    return tablename.replace('cordis_', '')[:-1].title()


def table_from_fk(fks):
    return [fk.column.table.name for fk in fks
            if fk.column.table.name != 'cordis_projects'][0]


def get_row(session, parent_orm, orm, data_row):
    (pk,) = inspect(parent_orm).primary_key
    (orm_pk,) = [c for c in orm.__table__.columns
                 for fk in c.foreign_keys
                 if fk.column.table.name == parent_orm.__tablename__]
    condition = (pk == data_row[orm_pk.name])
    _row = session.query(parent_orm).filter(condition).first()
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
        orm, parent_orm, rel_name = prepare_base_entities(table)
        with graph_session(**gkwargs) as tx:
            for db, orm_instance in db_session_query(query=orm, engine=engine,
                                                     limit=limit):
                orm_to_neo4j(session=db, transaction=tx,
                             orm_instance=orm_instance,
                             parent_orm=parent_orm, rel_name=rel_name)
