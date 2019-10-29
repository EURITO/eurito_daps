import logging
from py2neo import Node, NodeMatcher, Relationship

#
#
def get_index(node, graph, igr):
    '''A utility function, which takes a py2neo node and a graph, returns the index of the node in igraph. If the node does not exist, create new node and return index.

    Args:
        node: py2neo Node object
        graph: py2neo Neo4j graph connection
        igr: igraph instance

    Returns:
        position: an index of the created or found node in the igraph
        igr: igraph instance
    '''

    try:
        igr_index = igr.vs.find(gid=node.identity).index
        return igr_index, igr
    except:
        igr.add_vertices(1)
        position = len(igr.vs)-1
        #copy neo4j contents into igraph
        igr.vs[position]['gid'] = node.identity
        for key in node.keys():
            igr.vs[position][key] = node[key]
        return position, igr


def add_betw_property(graph, igraph, betw):
    '''A utility function, which adds calculated betweenness property to each node in Neo4j graph and commits the related transaction to the database

    Args:
        graph: py2neo Neo4j graph connection
        igraph: igraph instance
        betw: array of calculated betweenness centralitites for each node in igraph
    '''

    for index, v in enumerate(igraph.vs):
        neo_node = graph.nodes.get(v['gid'])
        neo_node["betw"] = betw[index]
        graph.merge(neo_node)
        graph.push(neo_node)
