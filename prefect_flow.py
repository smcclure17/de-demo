from prefect import task, Flow, Parameter, case
from prefect.schedules import IntervalSchedule
import sqlalchemy as sq
import pandas as pd
from datetime import timedelta as td, datetime as dt
import json


@task
def extract() -> tuple:
    """
        retrieve data in raw json form from file
        returns:
            data: pd.Dataframe of raw data
            log: int of the run-id 
    """
    # pretend that were getting this data from an API or something cool
    with open('data/la_vacc_03.json') as f:
        jsons = json.load(f)
    log = jsons["runid"]
    data = pd.DataFrame(jsons['result']['records'])
    return data, log

@task
def start_engine(name: str) -> sq.engine.base.Engine:
    """ 
        creates a connection to a database (in the same directory) with specified name
        name: string of database name (without db extension)
        returns: sq.engine.base.Engine linked to the specified sqlite database
    """
    engine = sq.create_engine('sqlite:///{}.db'.format(name), echo=True)
    return engine

@task
def check_if_new(data: int, eng: sq.engine.base.Engine) -> bool:
    """
        checks to see if the run_id matches any of the previous runs. 
        params: runid/log value of the raw data
        return: True if log is unique, and data should be appended to db
                False if log is not unique, (data is duplicated)
    """
    log_vals = list(pd.read_sql("select * from log", eng)['runid'])
    if data not in log_vals: return True
    else: return False

@task
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
        reshape the data into a form matching that of the schema of the db.
        params: pd.DataFrame containing raw data
        return: pd.DataFrame of normalized/reshaped data
    """
    # filter the data we want, match col names to database
    data = data[["county", "administered_date", "partially_vaccinated", "fully_vaccinated"]]
    # rename columns
    data.columns = ['county', 'date', 'initiated', 'completed']
    return data

@task 
def load(data: pd.DataFrame, log: str, eng: sq.engine.base.Engine) -> None:
    """
        inserts dataframe values into covid_vaccines table and runid into runid table
        params: 
            data: pd.Dataframe of prepped data
            log: run_id of data
            eng: engine connected to database
        returns: None
    """
    # append covid data into corresponding table
    data.to_sql('covid_vaccines', eng, index=False, if_exists='append')
    
    # append run id to list of updates
    log_row = pd.DataFrame({"runid":[log,]})
    log_row.to_sql('log', eng, index=False, if_exists='append')
    return None

def main():
    # initiate flow to run every minute
    schedule = IntervalSchedule(
        start_date=dt.utcnow() + td(seconds=1),
        interval=td(minutes=1),
    )
    with Flow("Basic-ETL", schedule) as flow:
        fetched_data = extract()
        engine = start_engine('data')
        check_duplicate = check_if_new(fetched_data[1], engine)

        # only add data if not duplicated
        with case(check_duplicate, True):
            prepped_data = transform(fetched_data[0])
            load(prepped_data, fetched_data[1], engine)
        
        flow.run()

if __name__ == "__main__":
    main()
