'''A utility script, which outputs records from the specified tables in the database
'''

from eurito_daps.packages.utils import openaire_utils
from nesta.production.orms.orm_utils import get_mysql_engine

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from sqlalchemy import MetaData, Table

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import MetaData, Table
import logging

#database setup
Base = declarative_base()

association_table = Table('association', Base.metadata,
    Column('software_id', Integer, ForeignKey('softwareRecords.id')),
    Column('project_id', Integer, ForeignKey('ECProjectRecords.id'))
)

class SoftwareRecord(Base):
    __tablename__ = 'softwareRecords'

    id = Column(Integer, primary_key=True)
    title = Column(String(255))
    pid = Column(String(255))
    creators = Column(String(255))
    #projectcodes = Column(String(255))
    projects = relationship('ECProjectRecord',
                            secondary = association_table,
                            backref="software")

class ECProjectRecord(Base):
    __tablename__ = 'ECProjectRecords'

    id = Column(Integer, primary_key=True)
    title = Column(String(255))
    projectcode = Column(String(255))

engine = get_mysql_engine("MYSQLDBCONF", 'mysqldb', 'dev')

Base.metadata.create_all(engine)

#open DB session
Session = sessionmaker(bind=engine)
dbsession = Session()

#records = dbsession.query(ECProjectRecord).all()
records = dbsession.query(SoftwareRecord).all()

from sqlalchemy import func

# count User records, without
# using a subquery.
#project_count = dbsession.query(func.count(ECProjectRecord.id))
record_count = 0
for record in records:
    #logging.info('project is found: ', record.id, record.title, record.projectcode)
    record_count += 1

print("Found ", record_count, " records")

#close DB session
dbsession.close()

print(engine.table_names())
