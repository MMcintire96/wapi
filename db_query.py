import sqlite3
import json

def get_load_connection():
    '''Gets the connection to sqlite3 db'''
    conn = sqlite3.connect('db/data.db')
    c = conn.cursor()
    return conn.cursor()

c = get_load_connection()

def query(row):
    '''Querys the data, check db rows for params'''
    if row == "all":
        c.execute("SELECT * FROM data")
        data = []
        for x in c.fetchall():
            d = {
                'attributes': x[1],
                'datatype': x[2],
                'date': x[3],
                'station': x[4],
                'value': x[5],
                'state': x[6],
                }
            data.append(d)
        return data
    c.execute(f"SELECT {row} FROM data")
    if ',' in row:
        return [list(x) for x in c.fetchall()]
    else:
        return [list(x[0]) for x in c.fetchall()]



data = query("date, value, state")
states = set([]) ;[states.add(d[2]) for d in data]
years = set([]) ;[years.add(str(d[0][:4])) for d in data]

years = sorted(list(years))
states = sorted(list(states))

def get_percp(year, state):
    percp = 0
    total = 0
    for d in data:
        if d[0][:4] == year and d[2] == state:
            percp += d[1]
            total += 1
    return round(percp/total, 2)


d_tot = []
for state in states:
    d = []
    s = {'state': state, 'data': d}
    for year in years:
        d.append({'year': year, 'percp': get_percp(year, state)})
    d_tot.append(s)
print(json.dumps(d_tot[0]['data'], indent=2))

# fix this shit
#[
#    {'state': illionis, 'data': [{'year': year, 'avg_percp': percp]}
#]


if __name__ == "__main__":
    pass

