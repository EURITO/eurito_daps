from bs4 import BeautifulSoup
import re
from eurito_daps.core.orms import openaire_orm
import pdb
import logging
import time


def link_record_with_project(record, record_obj, db_session):
    '''A utility function, which links records with related EC projects and stores this linkage in the association table in the database

    Args:
        record (dict): contains metadata of a record (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        record_obj (ORM Object): a record returned by a query from the database (could be software, dataset, publication or ECProject record)
    '''
    #extract project codes
    project_codes = record['project_codes']
    #for each project code, create projectcode object,
    #add a relationship between this projectcode and softwareObj in association table
    for project_code in project_codes:
        #find project with this code and create an object
        #pdb.set_trace()
        project_obj = find_project_in_db(project_code, db_session)
        #if there is a related project, create a relationship between current software and found project
        if project_obj:
            project_obj.software.append(record_obj)

    return record_obj

def find_project_in_db(in_project_code, db_session):
    '''A utility function, which returns ORM-type record objects from the database with the specified EC project code

    Args:
        in_project_code (int): EC 6-digit project code
    '''

    records = db_session \
                     .query(openaire_orm.ECProjectRecord) \
                     .filter_by(project_code=in_project_code)

    #pdb.set_trace()

    try:
        return records[0]
    except IndexError:
        return None

def write_records_to_db(records, output_type, db_session):
    '''A utility function, which writes specified records into the database

    Args:
        records (list of dicts): stores metadata of records (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        output_type (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        db_session (instance of sessionmaker Session): current database session
    '''

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
    '''A utility function, which returns a ORM-type object according to the specified output type

    Args:
        record (dict): contains metadata of a record (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        output_type (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
    '''
    if output_type == 'software':
        return openaire_orm.SoftwareRecord(title=cur_record['title'], pid=cur_record['pid'], creators=str(cur_record['creators']) )
    if output_type == 'ECProjects':
        return openaire_orm.ECProjectRecord(title=cur_record['title'], project_code=cur_record['project_code'])

#TODO add research datasets and other research projects parser

def parse_soft (cur_soup):
    '''A utility function, which parses software records from XML and returns a list of records with software that are related to EC Projects

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:result"))

    return [{'project_codes': r.find('code'),
         'pid': r.find('pid').text,
         'title': r.find('title').text,
         'creators': r.find_all('creators'),}
         for r in results
         if r.find_all('code')] #if code tag exists, then it is related to EC project

def parse_proj (cur_soup):
    '''A utility function, which parses EC project records from XML, returns a list of records

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:project"))

    return [{'title': r.find('title').text,
         'project_code': r.find('code').text}
         for r in results]


def get_res_token(soup):
    '''A utility function, which extracts resumption token from XML, returns a string containing resumption token

    Args:
        soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''

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

def get_soup_contents(currentUrl, reqsession, output_type, resumption_token):
    '''A utility function, which returns BeautifulSoup content in XML format from the given URL

    Args:
        currentUrl (string): contains API request URL
        reqsession (instance of Requests session): currently open HTTP request
    '''

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
            logging.info("Service unavailable, waiting 10 seconds and trying again")
            time.sleep(10)
            continue
