from bs4 import BeautifulSoup
import re
from eurito_daps.production.orms import openaire_orm
from eurito_daps.packages.utils import globals


def link_record_with_project(record, record_obj):
    '''A utility function, which links records with related EC projects and stores this linkage in the association table in the database

    Args:
        record (dict): contains metadata of a record (e.g. record of type "software" would store fields such as "title", "pid", "creator", etc.)
        record_obj (ORM Object): a record returned by a query from the database (could be software, dataset, publication or ECProject record)
    '''
    #extract project codes
    project_codes = record['projectcodes']
    #for each project code, create projectcode object,
    #add a relationship between this projectcode and softwareObj in association table
    for project_code in project_codes:
        #find project with this code and create an object
        project_obj = find_project_in_db(project_code.text)
        #if there is a related project, create a relationship between current software and found project
        if project_obj:
            project_obj.software.append(record_obj)

    return record_obj

def find_project_in_db(in_project_code):
    '''A utility function, which returns ORM-type record objects from the database with the specified EC project code

    Args:
        in_project_code (int): EC 6-digit project code
    '''

    records = globals.db_session.query(openaire_orm.ECProjectRecord).filter_by(project_code=in_project_code)

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
    #iterate through records
    for record in records:

        #create object
        record_obj = get_record_object(record, output_type)

        #if software, find related EC projects and create relationship with related ECprojects via association table
        if output_type == "software":
            record_obj = link_record_with_project(record, record_obj)

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
        return openaire_orm.ECProjectRecord(title=cur_record['title'], project_code=cur_record['projectcode'])


def parse_software_soup_rt (cur_soup):
    '''A utility function, which parses software records from XML and returns a list of records with software that are related to EC Projects

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:result"))
    for result in results:

        #check if related to EC projects
        project_codes = result.find_all('code')
        #if code tag exists, then it is related to EC project
        if project_codes:
            out_obj = dict()
            out_obj['projectcodes'] = project_codes

            pid = result.find('pid') #doi identifier

            out_obj['pid'] = pid.text

            title = result.find('title')

            out_obj['title'] = title.text

            creators = result.find_all('creator')

            out_obj['creators'] = creators

            output_list.append(out_obj)
    return output_list

def parse_projects_soup_rt (cur_soup):
    '''A utility function, which parses EC project records from XML, returns a list of records

    Args:
        cur_soup (XML string): contains string formatted in XML, that was obtained from BeautifulSoup request to the API
    '''
    output_list = list()
    results = cur_soup.find_all(re.compile("^oaf:project"))
    for result in results:
        out_obj = dict()

        title = result.find('title')

        out_obj['title'] = title.text

        project_code = result.find('code')

        out_obj['projectcode'] = project_code.text

        output_list.append(out_obj)
    return output_list


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
