import logging
from py2neo import Node, NodeMatcher, Relationship

def add_betw_property(graph, igraph, betw):
    '''A utility function, which adds calculated betweenness property to each node in Neo4j graph and commits the related transaction to the database

    Args:
        graph: py2neo Neo4j graph connection
        igraph: igraph instance
        betw: array of calculated betweenness centralitites for each node in igraph
    '''

    for index, v in enumerate(igraph.vs):
        graph.run(
            "MATCH (n) WHERE ID(n) = {nodeid} SET n.betw = {betw}",
            {"nodeid": v['name'], "betw": betw[index]}
        )
