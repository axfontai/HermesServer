import pandas as pd
import sqlite3 as sql


# dir dans lequel est installé la database sqlite3
SQLiteDir = '/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux/SQlite'
# se connecte se la base de donnee sqlite3 Hermes
conn = sql.connect(SQLiteDir + '/Hermes2018.db')
cur = conn.cursor()
# liste des b3s dans la base de donnée
B3S_DF = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
B3S_list = list(B3S_DF['name'])


for b3s in B3S_list:

    sql = "DELETE FROM {b3s} WHERE Jour NOT BETWEEN '2018-01-01' AND '2019-01-01'; ".format(b3s=b3s)
    cur.execute(sql)
    print(sql)

    #cur.execute("SELECT * FROM {b3s} ORDER BY Jour ASC;".format(b3s=b3s))

    conn.commit()

cur.execute("VACUUM;")

conn.close()