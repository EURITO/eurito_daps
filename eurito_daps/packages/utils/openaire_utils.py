from bs4 import BeautifulSoup
import re
from eurito_daps.production.orms import openaire_orm
from eurito_daps.packages.utils import globals


def link_record_with_project(record, recordObj):
    '''A utility function, which links records with related EC projects and stores this linkage in the association table in the database

    Args:
        record (dict): contains metadata of a record (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        recordObj (ORM Object): a record returned by a query from the database (could be software, dataset, publication or ECProject record)
    '''
    #extract project codes
    projectcodes = record['projectcodes']
    #for each project code, create projectcode object,
    #add a relationship between this projectcode and softwareObj in association table
    for projectcode in projectcodes:
        #find project with this code and create an object
        projectObj = find_project_in_db(projectcode.text)
        #if there is a related project, create a relationship between current software and found project
        if projectObj:
            #print ("projectObj.id exists", projectObj.id)
            projectObj.software.append(recordObj)

    return recordObj

def find_project_in_db(inprojectcode):
    '''A utility function, which returns ORM-type record objects from the database with the specified EC project code

    Args:
        inprojectcode (int): EC 6-digit project code
    '''

    records = globals.dbsession.query(openaire_orm.ECProjectRecord).filter_by(projectcode=inprojectcode)

    if records.count() > 0:
        return records[0]
    else:
        return None

def write_records_to_db(records, outputType, dbsession):
    '''A utility function, which writes specified records into the database

    Args:
        records (list of dicts): stores metadata of records (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        outputType (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
        dbsession (instance of sessionmaker Session): current database session
    '''
    #iterate through records
    for record in records:

        #create object
        recordObj = get_record_object(record, outputType)

        #if software, find related EC projects and create relationship with related ECprojects via association table
        if outputType == "software":
            recordObj = link_record_with_project(record, recordObj)

        #add object into database
        #dbsession.add(recordObj) #this gives an error, try incorporating this:
        local_object = dbsession.merge(recordObj)
        dbsession.add(local_object)

        dbsession.commit()

def get_record_object(cur_record, outputType):
    '''A utility function, which returns a ORM-type object according to the specified output type

    Args:
        record (dict): contains metadata of a record (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        outputType (str): type of record to be extracted from OpenAIRE API. Accepts "software", "datasets", "publications", "ECProjects"
    '''
    if outputType == 'software':
        return openaire_orm.SoftwareRecord(title=cur_record['title'], pid=cur_record['pid'], creators=str(cur_record['creators']) )
    if outputType == 'ECProjects':
        return openaire_orm.ECProjectRecord(title=cur_record['title'], projectcode=cur_record['projectcode'])


def parse_software_soup_rt (cur_soup):
    '''A utility function, which parses software records from XML and returns a list of records with software that are related to EC Projects

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    outputlist = list()
    results = cur_soup.find_all(re.compile("^oaf:result"))
    for result in results:

        #check if related to EC projects
        projectcodes = result.find_all('code')
        #print(projectcodes)
        #if code tag exists, then it is related to EC project
        if projectcodes:
            out_obj = dict()
            out_obj['projectcodes'] = projectcodes

            pid = result.find('pid') #doi identifier
            #print(pid.text)
            out_obj['pid'] = pid.text

            title = result.find('title')
            #print(title.text)
            out_obj['title'] = title.text

            creators = result.find_all('creator')
            #print(creators)
            out_obj['creators'] = creators

            outputlist.append(out_obj)
    return outputlist

def parse_projects_soup_rt (cur_soup):
    '''A utility function, which parses EC project records from XML, returns a list of records

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    outputlist = list()
    results = cur_soup.find_all(re.compile("^oaf:project"))
    for result in results:
        out_obj = dict()

        title = result.find('title')
        #print(title.text)
        out_obj['title'] = title.text

        projectcode = result.find('code')
        #print(projectcode)
        out_obj['projectcode'] = projectcode.text

        outputlist.append(out_obj)
    return outputlist


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

def get_soup_contents(currentUrl, reqsession):
    '''A utility function, which returns BeautifulSoup content in XML format from the given URL

    Args:
        currentUrl (string): contains API request URL
        reqsession (instance of Requests session): currently open HTTP request
    '''

    response = reqsession.get(currentUrl)
    soup = BeautifulSoup(response.content, 'lxml')
    response.close()
    return soup
