from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import MetaData, Table

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
