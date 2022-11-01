import sqlalchemy as sql, sqlalchemy.ext.declarative as declarative, sqlalchemy.orm as orm, pyodbc

#cx_Oracle.init_oracle_client(lib_dir=r"C:/Oracle_64/product/11.2.0/client_1/BIN/")

MM_DATABASE_URL = "sqlite://///10.240.160.176\g$\Data/MMRepo.db3"

ADMS_DATABASE_URL = "mssql+pyodbc://EpsaReportes:cmXoasys2@10.238.109.61\OASYSHDB:20010/ADMS_QueryEngine?driver=SQL Server"

# GIS_DATABASE_URL = "oracle+cx_oracle://RDJARAMILLO:cmXoasys17@PV10262/arcgis"

engine_datawarehouse = sql.create_engine(
    MM_DATABASE_URL, 
    connect_args={"check_same_thread": False},
)

SessionLocal = orm.sessionmaker(autocommit=False, autoflush=False, bind=engine_datawarehouse)

Base = declarative.declarative_base()

engine_adms = sql.create_engine(
    ADMS_DATABASE_URL
    )

# engine_adms = sql.create_engine(
#     ADMS_DATABASE_URL
# )

# engine_datawarehouse = sql.create_engine(
#     GIS_DATABASE_URL
# )

