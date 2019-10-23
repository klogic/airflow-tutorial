from datetime import datetime
from random import randrange

from sqlalchemy import DECIMAL, Column, DateTime, Integer, MetaData, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

from connection import conn

Base = declarative_base()

meta = MetaData(conn).reflect()

dwhConnection = conn.connect()
SessionDwh = sessionmaker(bind=dwhConnection)
sessionDwh = SessionDwh()


class BaseTable(Base):
    __tablename__ = 'sales_order'
    entity_id = Column(Integer, autoincrement=True, primary_key=True)
    amount = Column(DECIMAL(14, 4), nullable=False)
    created_at = Column(DateTime(), nullable=False)


def initSalesTable():
    isRun = False
    if not conn.dialect.has_table(conn, 'sales_order'):
        Base.metadata.create_all(bind=conn)
        sessionDwh.commit()
        isRun = True
    return isRun


def insertRandomSalesOrder():
    prepareData = []
    now = datetime.now()
    Base.metadata.create_all(bind=conn)
    prepareData.append(BaseTable(
        amount=randrange(1, 1000),
        created_at=now
    ))
    sessionDwh.add_all(prepareData)
    sessionDwh.commit()
    return True


initSalesTable()
insertRandomSalesOrder()

sessionDwh.close()
dwhConnection.close()
