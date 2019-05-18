import api
import datetime
import pandas as pd
from sqlalchemy import create_engine


states = [{'state': 'Illinois',  'FIPS': 17},
        {'state': 'Iowa', 'FIPS': 19},
        {'state': 'Minnesota', 'FIPS': 27},
        {'state': 'Indiana', 'FIPS': 18},
        {'state': 'Nebraska', 'FIPS': 31},
        {'state': 'Ohio', 'FIPS': 39},
        {'state': 'Missouri', 'FIPS': 29},
        {'state': 'South Dakota', 'FIPS': 46},
        {'state': 'North Dakota', 'FIPS':38},
        {'state': 'Kansas', 'FIPS': 20},
        {'state': 'Wisconsin', 'FIPS': 55}]


engine = create_engine('sqlite:///db/d2.db', echo=False)

def main():
    '''Loops through the states and adds data to sqlite'''
    total = datetime.datetime.now()
    for s in states:
        try:
            offset = 0
            while True:
                d = api.DataAPI()
                data = d.yield_data(datasetid="GSOM",
                                units="metric",
                                datatypeid="PRCP",
                                locationid=f"FIPS:{s['FIPS']}",
                                startdate="1970-01-01",
                                enddate="1979-12-31",
                                limit="1000", offset=f"{offset}")
                df = pd.DataFrame(data)
                df['state'] = s['state']
                df.to_sql('data', con=engine, if_exists='append')
                offset += 1000
                print(f' ----------------- \n {s["state"]} {s["FIPS"]} {offset} \n -----------------')
        except Exception as e:
            print(f'EXECPTION : {e}')
            pass
    print(datetime.datetime.now() - total)

if __name__ == '__main__':
    main()

