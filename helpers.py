import sqlalchemy as sq
from sqlalchemy import Table, Column, Integer, String, MetaData


def create_db(name):
    engine = sq.create_engine('sqlite:///{}.db'.format(name), echo=True)    
    meta = sq.MetaData()

    # define tables
    covid = Table(
        'covid_vaccines', meta, 
        Column('county', String), 
        Column('date', Integer),
        Column('initiated', Integer),
        Column('completed', Integer),
    )
    log = Table(
        'log', meta,
        Column('runid', Integer, primary_key = True),
    )

    m = meta.create_all(engine)
    return engine

create_db('data')