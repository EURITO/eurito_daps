from bs4 import BeautifulSoup
import logging
import time
from py2neo import Node


def write_record_to_neo(record, output_type, graph):
    '''A utility function, which takes record and writes it to neo4j graph

    Args:
        record (dict): a dictionary that contains metadata about a record
        output_type(str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        graph(graph_session): connection to neo4j database
    '''

    record_type = str(output_type).capitalize()

    found_node = graph.nodes.match(output_type, pid=record['pid']).first()

    if found_node is None:
        created_node = Node(record_type, title=record['title'], pid=record['pid'])
        graph.create(created_node)
        return created_node
    else:
        logging.info("returning found node")
        return found_node

def get_project_soups(currentUrl, reqsession, output_type, projectID):
    ''' Gets a beautiful soup according to output type and projectID

        Args:
            currentUrl(str): URL to OpenAIRE API
            reqsession (instance of Requests session): currently open HTTP request
            output_type(str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
            projectID(str): EC project identifier

        Returns:
            souplist(list): a list of BeautifulSoup objects that contain the results from API call
    '''

    response_size = -1
    total = 9999
    page = 1
    pagesize = 100
    souplist = list()
    while (page + 1)*pagesize < total:
        response = reqsession.get(currentUrl + output_type, params={'hasProject': 'true', 'projectID': projectID, 'size': pagesize, 'page': page})
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
            logging.info(response.status_code)
            logging.info("Service unavailable, waiting 10 seconds and trying again")
            time.sleep(10)
            continue
    return souplist

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

'''
def get_soup_contents(currentUrl, reqsession, output_type, resumption_token):

    for x in range(0, 9):
        #requests.get(url, params={'metadataPrefix':'oaf', 'set':output_type})
        #pdb.set_trace()
        if resumption_token == 'None' or resumption_token == 'First request':
            response = reqsession.get(currentUrl, params={'verb': 'ListRecords', 'metadataPrefix': 'oaf', 'set': output_type})
            logging.info("No resumptionToken")
        else:
            logging.info("resumptionToken is there ")
            #response = reqsession.get(currentUrl, params={'verb': 'ListRecords', 'metadataPrefix': 'oaf', 'set': output_type, 'resumptionToken': resumption_token})
            #resumptionToken as a parameter does not work for requests call, hence use string request
            requeststr = currentUrl + '?verb=ListRecords&resumptionToken=' + resumption_token
            logging.info(requeststr)
            response = reqsession.get(requeststr)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'lxml')
            response.close()
            return soup
        else:
            logging.info(response.status_code)
            logging.info("Service unavailable, waiting 10 seconds and trying again")
            time.sleep(10)
            continue

def add_linkages_to_neo(record, output_type, graph):

    cypherquery = " MATCH (b:Project),(a:Dataset) WHERE a.id = %s AND ID(b) = %s CREATE (b)-[r:hasDataset]->(a) RETURN r" % (project_codes, record['id'])

    graph.run(cypherquery)

def find_project_in_db(in_project_code, db_session):

    records = db_session \
                     .query(openaire_orm.ECProjectRecord) \
                     .filter_by(project_code=in_project_code)

    #pdb.set_trace()

    try:
        return records[0]
    except IndexError:
        return None

def write_records_to_db(records, output_type, db_session):

    if output_type == "software":
        is_software = True
    else:
        is_software = False
    #iterate through records
    for record in records:

        #create object
        record_obj = get_record_object(record, output_type)

        #if software, find related EC projects and create relationship with related ECprojects via association table
        if is_software:
            record_obj = link_record_with_project(record, record_obj, db_session)

        #add object into database
        local_object = db_session.merge(record_obj)
        db_session.add(local_object)

        db_session.commit()

def get_record_object(cur_record, output_type):

    if output_type == 'software':
        return openaire_orm.SoftwareRecord(title=cur_record['title'], pid=cur_record['pid'], creators=str(cur_record['creators']) )
    if output_type == 'ECProjects':
        return openaire_orm.ECProjectRecord(title=cur_record['title'], project_code=cur_record['project_code'])

def parse_soft (cur_soup):
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:result"))

    return [{'project_codes': r.find('code'),
         'pid': r.find('pid').text,
         'title': r.find('title').text,
         'creators': r.find_all('creators'),}
         for r in results
         if r.find_all('code')] #if code tag exists, then it is related to EC project

def parse_proj (cur_soup):

    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:project"))

    return [{'title': r.find('title').text,
         'project_code': r.find('code').text}
         for r in results]

def parse_datasets (cur_soup):
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:result"))

    return [{'project_codes': r.find('code').text,
        'title': r.find('title').text,
        'pid': r.find('pid').text,}
         for r in results
         if r.find_all('code')] #if code tag exists, then it is related to EC project

def get_res_token(soup):
    with open('current_soup.txt', 'w',  encoding="utf-8") as f:
            f.write(str(soup))
    res_token = soup.find(re.compile("^oai:resumptiontoken"))

    if res_token:
        res_token_str = res_token.text
        res_token_str = res_token_str.replace(' ', '%20')
        res_token_str = res_token_str.replace('"', '%22')
        return res_token_str
    else:
        return 'None'
'''
