import helpers as h
import pandas as pd


def insert_into(df, table, engine):
    df.to_sql(table, engine, index=False, if_exists='append')
    return True

if __name__ == '__main__':
    rows = [{'fips':23, 'county':'johnson', 'state':'AL', 'completed':3432},
            {'fips':12, 'county':'billiam', 'state':'NH', 'completed':42311}]
    
    # # h.create_db('data')
    # engine = h.start_engine('data').connect()
    # df = pd.DataFrame(rows)
    # print(df)
    # insert_into(df, 'covid_vacines', engine)