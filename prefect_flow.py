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
    # pretend that were getting this data from an API or something cool and that it would update every now and then
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
def check_if_new(runid: int, eng: sq.engine.base.Engine) -> bool:
    """
        checks to see if the run_id matches any of the previous runs. 
        params: runid/log value of the raw data
        return: True if log is unique, and data should be appended to db
                False if log is not unique, (data is duplicated)
    """
    log = list(pd.read_sql("select runid from log", eng)['runid'])
    if runid not in log: return True
    else: return False

@task
def transform(data: pd.DataFrame) -> pd.DataFrame:
    """
        reshape the data into a form matching that of the schema of the db.
        params: pd.DataFrame containing raw data
        return: pd.DataFrame of normalized/reshaped data
    """
    # filter only the columns that we care about
    data = data[["county", "administered_date", "partially_vaccinated", "fully_vaccinated"]]
    # rename the columns to match our database schema
    data.columns = ['county', 'date', 'initiated', 'completed']
    return data

@task 
def load(data: pd.DataFrame, runid: str, eng: sq.engine.base.Engine) -> None:
    """
        inserts dataframe values into covid_vaccines table and runid into runid table
        params: 
            data: pd.Dataframe of prepped data
            log: run_id of data
            eng: engine connected to database
        returns: None
    """
    # insert covid data into corresponding table
    data.to_sql('covid_vaccines', eng, index=False, if_exists='append')
    
    # insert runid to list of updates
    runid_row = pd.DataFrame({"runid":[runid,]})
    runid_row.to_sql('log', eng, index=False, if_exists='append')
    return None


def main():

    # create a schedule with a 1 minute interval
    schedule = IntervalSchedule(
        start_date=dt.utcnow() + td(seconds=1),
        interval=td(minutes=1),
    )

    # initiate flow to run every minute
    with Flow("Basic-ETL", schedule) as flow:

        # define the order of each task
        fetched_data = extract()
        engine = start_engine('data')
        check_duplicate = check_if_new(fetched_data[1], engine)

        # only manipulate and add data if not duplicated
        with case(check_duplicate, True):
            prepped_data = transform(fetched_data[0])
            load(prepped_data, fetched_data[1], engine)
        
        flow.run()


if __name__ == "__main__":
    main()
