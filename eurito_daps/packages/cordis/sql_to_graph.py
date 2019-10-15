import logging
import py2neo
from sqlalchemy.orm import sessionmaker

from eurito_daps.core.ogms.cordis_h2020_ogm import FrameworkProgramme
from eurito_daps.core.ogms.cordis_h2020_ogm import Organizations as OrganizationsGraph
from eurito_daps.core.ogms.cordis_h2020_ogm import Projects as ProjectsGraph
from eurito_daps.core.ogms.cordis_h2020_ogm import Programmes as ProgrammesGraph
from nesta.production.orms.cordis_h2020_orm import Organizations as OrganizationsSQL
from nesta.production.orms.cordis_h2020_orm import Projects as ProjectsSQL
from nesta.production.orms.orm_utils import db_session, get_mysql_engine
from nesta.production.luigihacks.misctools import get_config


def create_graph_node(NodeClass, row, columns=None):
    if columns is None:
        # TODO: get the list of properties from the node class instead, if possible
        columns = [c.key for c in row.__class__.__table__.columns]

    node = NodeClass()
    for column in columns:
        setattr(node, column, getattr(row, column))

    return node


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    logging.basicConfig(handlers=[log_stream_handler, ],
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    neo4j = get_config('neo4j.config', 'neo4j')
    graph = py2neo.Graph(host=neo4j['host'],
                         auth=(neo4j['user'], neo4j['password']),
                         secure=True)
    logging.warning('deleting existing graph')
    graph.delete_all()

    engine = get_mysql_engine('MYSQLDB', 'mysqldb', 'production')
    Session = sessionmaker(engine)

    # create H2020
    h2020 = FrameworkProgramme()
    h2020.id = 'H2020'
    h2020.name = 'Horizon 2020'
    graph.push(h2020)

    with db_session(engine) as session:
        for count, project in enumerate(session.query(ProjectsSQL).limit(100).all(), start=1):
            logging.debug(f'creating node for {project.rcn}')
            project_node = create_graph_node(ProjectsGraph, project)

            for organization in (session
                                 .query(OrganizationsSQL)
                                 .filter(OrganizationsSQL.project_rcn == project.rcn)
                                 .all()):
                # TODO: check for existing organizations and match
                # TODO: dont include the redundant role field
                org_node = create_graph_node(OrganizationsGraph, organization)
                org_node.participates_in.add(project_node,
                                             properties={'role': organization.role})
                graph.push(org_node)

            for programme in project.programmes:
                # TODO: duplicate programmes are created even though this should not be
                # possible. eg H2020-EU.3.1.1.
                # the check below is likely redundant as dupes still occur
                prog = (ProgrammesGraph
                        .match(graph)
                        .where(f"_.id='{programme.id}'")
                        .first())
                if not prog:
                    prog = create_graph_node(ProgrammesGraph, programme)
                    prog.part_of.add(h2020)
                project_node.part_of.add(prog)

            graph.push(project_node)
            if not count % 10:
                logging.info(f"{count}")
