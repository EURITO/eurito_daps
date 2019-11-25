from bs4 import BeautifulSoup
import re
from eurito_daps.core.orms import openaire_orm
from eurito_daps.core.orms.openaire_orm import Base, SoftwareRecord, association_table
import pdb
import logging
import time
from py2neo import Node, NodeMatcher

def write_record_to_neo(record, output_type, graph):
    '''A utility function, which takes record and writes it to neo4j graph

    Args:
        record (dict): a dictionary that contains metadata about a record
        output_type(str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        graph(graph_session): connection to neo4j database
    '''

    record_type = str(output_type).capitalize()

    found_node = graph.nodes.match(output_type, title=record['title']).first()
    if record['title'] == 'gCube 4.6.1 - Database Reosurce API v. 1.0.0':
        logging.info("returning created/found node %s " % found_node)

    if found_node == None:
        created_node = Node(record_type, title=record['title'], pid=record['pid'])
        graph.create(created_node)
        if record['title'] == 'gCube 4.6.1 - Database Reosurce API v. 1.0.0':
            logging.info("returning created node")
        return created_node
    else:
        if record['title'] == 'gCube 4.6.1 - Database Reosurce API v. 1.0.0':
            logging.info("returning found node")
        return found_node
    #gCube 4.6.1 - Database Reosurce API v. 1.0.0

def get_project_soups(currentUrl, reqsession, output_type, grant_num):
    ''' Gets a beautiful soup according to output type and grant number

        Args:
            currentUrl(str): URL to OpenAIRE API
            reqsession (instance of Requests session): currently open HTTP request
            output_type(str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
            grant_num(str): EC project grant number

        Returns:
            souplist(list): a list of BeautifulSoup objects that contain the results from API call
    '''

    response_size = -1
    total = 9999
    page = 1
    pagesize = 100
    souplist = list()
    while (page + 1)*pagesize < total:
        response = reqsession.get(currentUrl + output_type, params={'projectID': grant_num, 'size': pagesize, 'page': page})
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'lxml')
            #logging.info("soup retrieved for page %d" % page)
            souplist.append(soup)
            response.close()
            if page == 1:
                total = int(soup.find("total").text)
                #logging.info("total: %d" % total)
            page += 1
        else:
            logging.info(response)
            logging.info(response.status_code)
            logging.info("Service unavailable, waiting 10 seconds and trying again")
            time.sleep(10)
            continue
    if len(souplist) > 5:
        logging.info("retrieved %d soups for project %s" % (len(souplist), grant_num))
    return souplist

#TOO heavy on the API
'''def get_projectID(currentUrl, reqsession, acronym):

    response = reqsession.get(currentUrl + 'projects', params={'acronym': acronym})
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'lxml')
        #find grant number
        projectID = int(soup.find("code").text)
        #logging.info("acronym %s for grant number %d" % (acronym, projectID))
        response.close()
        return projectID
    else:
        logging.info(response)
        logging.info(response.status_code)
        logging.info("Service unavailable")
'''

def enrich_grant_num_neo4j(graph, reqsession, bulkURL):
    '''
    Enriches neo4j project nodes with grant number information
    '''
    total_dict = dict() #resulting dictionary
    response = reqsession.get(bulkURL, params={'verb': 'ListRecords', 'metadataPrefix': 'oaf', 'set': 'ECProjects'})
    first_soup = BeautifulSoup(response.content, 'lxml')
    resumption_token = get_res_token(first_soup)
    add_grant_num_property(first_soup, graph)
    logging.info('First resumption token is %s' % resumption_token)

    while resumption_token != 'None':
        #response = reqsession.get(bulkURL, params={'verb': 'ListRecords', 'resumptionToken' : resumption_token})
        response = reqsession.get(bulkURL + '?verb=ListRecords&resumptionToken=' + resumption_token)
        #logging.info('Next response is %s' % response)
        soup = BeautifulSoup(response.content, 'lxml')
        resumption_token = get_res_token(soup)
        logging.info('Next resumption token is %s' % resumption_token)
        add_grant_num_property(soup, graph)

def add_grant_num_property(soup, graph):
    '''A utility function, which adds project grant number to each node in Neo4j graph and commits the related transaction to the database

    Args:

    '''
    results = soup.find_all(re.compile("^oaf:project"))
    resultdict = dict()
    for result in results:
        acronym =  result.find('acronym').text.upper()
        grant_num =  result.find('code').text
        #logging.info("Searching for project %s with grant number %s " % (acronym, grant_num))
        matcher = NodeMatcher(graph)
        neo_node = matcher.match("Project", acronym=acronym).first()
        #logging.info(neo_node)
        if neo_node:
        #neo_node = graph.nodes.get(v['gid']) where acronym=acronym
            graph.merge(neo_node)
            neo_node["grant_num"] = grant_num
            graph.push(neo_node)
            #logging.info("Project %s with grant number %s is updated in neo4j" % (neo_node['acronym'], neo_node['grant_num']))
        else:
            logging.info("Project %s is not found in neo4j" % acronym)

def get_res_token(soup):
    '''A utility function, which extracts resumption token from XML, returns a string containing resumption token
    Args:
        soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''

    with open('current_soup.txt', 'w',  encoding="utf-8") as f:
            f.write(str(soup))
    res_token = soup.find(re.compile("^oai:resumptiontoken"))
    #print(str(res_token))
    if res_token:
        res_token_str = res_token.text
        res_token_str = res_token_str.replace(' ', '%20')
        return res_token_str
    else:
        return 'None'

def get_results_from_soups(souplist):
    ''' Extracts string from all BeautifulSoup objects and merges them into one list

        Args:
            souplist(list): a list of BeautifulSoup objects that contain the results from API call

        Returns:
            resultlist(list): a list of strings with results metadata
    '''
    resultlist = list()
    for soup in souplist:
        soupresults = soup.find_all("oaf:result")
        resultlist = resultlist + soupresults
    return resultlist
