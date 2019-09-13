from py2neo import NodeMatcher
from py2neo.data import Node, Relationship
from sqlalchemy.inspection import inspect
from nesta.core.orms.orm_utils import get_class_by_tablename, object_to_dict
from nesta.core.orms.cordis_orm import Base, Project
import logging


def orm_to_neo4j(session, transaction, orm_instance,
                 parent_orm=None, rel_name=None):
    """Pipe a SqlAlchemy ORM instance (a 'row' of data)
    to neo4j, inserting it as a node or relationship, as appropriate.

    Args:
        session (sqlalchemy.Session): SQL DB session.
        transaction (py2neo.Transaction): Neo4j transaction
        orm_instance (sqlalchemy.Base): Instance of a SqlAlchemy ORM, i.e.
                                        a 'row' of data.
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
                                                orm=orm,
                                                data_row=data_row,
                                                rel_name=rel_name,
                                                parent_orm=parent_orm)
        # Both nodes in the relationship were found
        if (fwd_rel, back_rel) != (None, None):
            transaction.create(fwd_rel)
            transaction.create(back_rel)
    # Otherwise create a single node, imposing constraints based on SQL FKs
    else:
        set_constraints(orm, graph.schema)
        transaction.create(Node(extract_name(orm.__tablename__), **data_row))


def build_relationships(session, graph, orm, data_row,
                        rel_name, parent_orm=None):
    """Build a py2neo.Relationship object from SqlAlchemy objects.x

    Args:
        session (sqlalchemy.Session): SQL DB session.
        transaction (py2neo.Transaction): Neo4j transaction
        orm (sqlalchemy.Base): A SqlAlchemy ORM
        rel_name (str): Name of the relationship to be added to Neo4j
        parent_orm (sqlalchemy.Base): Another ORM to build relationship to.
                                      If this is not specified, it implies
                                      that :obj:`orm` is node, rather than a
                                      relationship.
    """
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
    """Set constraints in the neo4j graph schema.

    Args:
        orm (sqlalchemy.Base): A SqlAlchemy ORM
        graph_schema (py2neo.Graph.Schema): Neo4j graph schema.
    """
    # Retrieve constraints by entity name
    entity_name = extract_name(orm.__tablename__)
    constraints = graph_schema.get_uniqueness_constraints(entity_name)
    # If no constraints have been applied, infer them from the PKs
    if len(constraints) == 0:
        (pk,) = inspect(orm).primary_key  # Assume only one constraint
        logging.info('Creating constraint on '
                     f'{entity_name}.{pk.name}')
        graph_schema.create_uniqueness_constraint(entity_name,
                                                  pk.name)
    # Otherwise don't re-register a constraint
    else:
        # Check that the constraint is consistent with having only one PK
        assert len(constraints) == 1


def prepare_base_entities(table):
    """Returns the objects required to generate a graph
    representation of the ORM.

    Args:
        table (sqlalchemy.sql.Table): SQL alchemy table object from which
                                      to extract an graph representation.
    Returns:
        {orm, parent_orm, rel_name}: Two ORMs and a string describing
                                     their relationship
    """
    # Retrieve foreign keys in order to infer a relationship
    fks = [fk for c in table.columns if c.foreign_keys
           for fk in c.foreign_keys]
    parent_orm, rel_name = None, None
    if len(fks) == 1:  # The relationship points to this table
        rel_name = f'HAS_{extract_name(table.name).upper()}'
    elif len(fks) == 2:  # The relationship points to a parent
        _tablename = table_from_fk(fks)
        rel_name = f'HAS_{extract_name(_tablename).upper()}'
        parent_orm = get_class_by_tablename(Base, _tablename)
    # Retrieve the ORM for this table
    orm = get_class_by_tablename(Base, table.name)
    return orm, parent_orm, rel_name


def flatten(orm_instance):
    """Convert a SqlAlchemy ORM (i.e. a 'row' of data) to flat JSON.

    Args:
        orm_instance (sqlalchemy.Base): Instance of a SqlAlchemy ORM, i.e.
                                        a 'row' of data.
    Returns:
        row (dict): A flat row of data, inferred from `orm_instance`
    """
    row = object_to_dict(orm_instance, shallow=True)
    return {k: _flatten(v) for k, v in row.items()}


def _flatten(data):
    """Flatten JSON to a flat dictionary via a hard-coded routine.

    Args:
        data (json-like): Input data.
    Returns:
        _data (dict): A flat dictionary.
    """
    # Flatten dictionaries directly
    if type(data) is dict:
        return flatten_dict(data)
    # if not a dict or a list, just return unaltered
    elif type(data) is not list:
        return data
    # Assume it is now a list. Check item types to determine next step.
    types = set(type(row) for row in data)
    if len(types) > 1:
        raise TypeError(f'Mixed types ({types}) are not accepted')
    # Flatten the internal data if the item types are dict
    try:
        if next(iter(types)) is dict:
            return [flatten_dict(row) for row in data]
    except StopIteration:  # Implies empty list
        pass
    # Otherwise don't flatten
    return data


def flatten_dict(row, keys=[('title',),
                            ('street', 'city', 'postalCode')]):
    """Flatten a dict by concatenating matching keys

    Args:
        row (dict): Data to be flattened
    Returns:
        flat (str): Concatenated data.
    """
    flat = ''
    for ks in keys:
        if not any(k in row for k in ks):
            continue
        flat = '\n'.join(row[k] for k in ks
                         if k in row)
        break
    if len(flat) == 0:
        print(row)
        assert False
    return flat


def retrieve_node(session, graph, orm, parent_orm, data_row):
    """Retrieve an existing node from neo4j, by first retrieving it's
    id (field name AND value) via SqlAlchemy.

    Args:
        session (sqlalchemy.Session): SQL DB session.
        transaction (py2neo.Transaction): Neo4j transaction
        orm (sqlalchemy.Base): SqlAlchemy ORM describing :obj:`data_row`
        parent_orm (sqlalchemy.Base): Parent ORM to build relationship to
        data_row (dict): Flat row of data retrieved from `orm`
    Returns:
        node (py2neo.Node): Node of data corresponding to data_row
    """
    row = get_row(session, parent_orm, orm, data_row)
    (pk,) = inspect(parent_orm).primary_key
    matcher = NodeMatcher(graph)
    return matcher.match(extract_name(parent_orm.__tablename__),
                         **{pk.name: row[pk.name]}).first()


def extract_name(tablename):
    """Convert a Cordis tablename to it's Neo4j Node label"""
    return tablename.replace('cordis_', '')[:-1].title()


def table_from_fk(fks):
    """Get the table name of the fk constraint, ignoring
    the cordis_projects table"""
    return [fk.column.table.name for fk in fks
            if fk.column.table.name != 'cordis_projects'][0]


def get_row(session, parent_orm, orm, data_row):
    """Retrieve a flat row of data corresponding to the parent
    relation, inferred via foreign keys.

    Args:
        session (sqlalchemy.Session): SQL DB session.
        parent_orm (sqlalchemy.Base): Parent ORM to build relationship to
        orm (sqlalchemy.Base): SqlAlchemy ORM describing :obj:`data_row`
        data_row (dict): Flat row of data retrieved from `orm`
    Returns:
        _row (dict): Flat row of data retrieved from `parent_orm`
    """
    (pk,) = inspect(parent_orm).primary_key
    (orm_pk,) = [c for c in orm.__table__.columns
                 for fk in c.foreign_keys
                 if fk.column.table.name == parent_orm.__tablename__]
    condition = (pk == data_row[orm_pk.name])
    _row = session.query(parent_orm).filter(condition).first()
    return flatten(_row)
