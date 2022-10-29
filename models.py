import sqlalchemy as sql, datetime as dt, database as db
from typing import Optional




class GIS_Jobs(db.Base):
    __tablename__ = "GIS_Jobs"
    JobName = sql.Column(sql.String, primary_key = True, index = True)
    Id= sql.Column(sql.String, unique = True, index = True)
    Version = sql.Column(sql.String)
    Description = sql.Column(sql.String)
    CurrentStatus = sql.Column(sql.String)
    Notes = sql.Column(sql.String)
    Owner = sql.Column(sql.String)


class NE_Executions(db.Base):
    __tablename__ = "Network_Exporter_Executions"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    GisJobId= sql.Column(sql.String,sql.ForeignKey("GIS_Jobs.JobName"))
    Version = sql.Column(sql.String)
    FeederList = sql.Column(sql.String)
    KindExecution = sql.Column(sql.String)
    ReceivedTime = sql.Column(sql.DateTime)
    ExecutionTime = sql.Column(sql.DateTime)
    Executed = sql.Column(sql.Boolean, default= False)
    Machine = sql.Column(sql.String)

class Extracts(db.Base):
    __tablename__ = "Extracts"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    extract_alias = sql.Column(sql.String)
    state_name = sql.Column(sql.String)
    source_name = sql.Column(sql.String)
    description = sql.Column(sql.String)
    last_modified_time = sql.Column(sql.DateTime)
    created_time = sql.Column(sql.DateTime)
    filename = sql.Column(sql.String)
    comment = sql.Column(sql.String)

class Extracts_History(db.Base):
    __tablename__ = "Extracts_History"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    extract_Id = sql.Column(sql.String, sql.ForeignKey("Extracts.Id"),index=True)
    transition_time = sql.Column(sql.DateTime,index=True)
    old_state = sql.Column(sql.String)
    new_state = sql.Column(sql.String)
    username = sql.Column(sql.String)
    comment = sql.Column(sql.String)

class NE_Extracts_Dependency(db.Base):
    __tablename__ = "NE_Extracts_Dependency"
    Id = sql.Column(sql.Integer, primary_key = True, index = True)
    execution_Id= sql.Column(sql.String,sql.ForeignKey("Network_Exporter_Executions.Id"),index= True)
    extract_Id= sql.Column(sql.String,sql.ForeignKey("Extracts.Id"),index= True)

class Changesets(db.Base):
    __tablename__ = "Changesets"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    changeset_type = sql.Column(sql.String)
    current_state = sql.Column(sql.String)
    created_date = sql.Column(sql.DateTime)
    last_modified_date = sql.Column(sql.DateTime)
    in_service = sql.Column(sql.DateTime)
    creator_username = sql.Column(sql.String)
    description = sql.Column(sql.String)

class Extracts_Changesets_Dependency(db.Base):
    __tablename__ = "Extracts_Changesets_Dependency"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    changeset_Id= sql.Column(sql.String,sql.ForeignKey("Changesets.Id"),index = True)
    extract_Id= sql.Column(sql.String,sql.ForeignKey("Extracts.Id"),index = True)


class Changesets_History(db.Base):
    __tablename__ = "Changesets_History"
    Id = sql.Column(sql.String, primary_key = True, index = True)
    changeset_Id = sql.Column(sql.String, sql.ForeignKey("Changesets.Id"))
    transition_time = sql.Column(sql.DateTime)
    old_state = sql.Column(sql.String)
    new_state = sql.Column(sql.String)
    username = sql.Column(sql.String)
    comment = sql.Column(sql.String)

