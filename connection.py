from airflow.models import Variable
from sqlalchemy import create_engine

host = Variable.get("host")
db_name = Variable.get("db_name")
username = Variable.get("username")
password = Variable.get("password")

connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'
                           .format(username=username, password=password,
                                   url=host, db_name=db_name), echo=False)
conn = connection.connect()
