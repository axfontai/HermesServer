import pandas as pd
import sqlite3 as sql
import os


os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu = os.getcwd()
DirCSV: str = DirTestCarrefoursFeu + "/MeldpuntenCSV"
DirSQlite = DirTestCarrefoursFeu + "/SQLite"

conn = sql.connect(DirSQlite + "/Meldpunten.db")
cur = conn.cursor()

B3S_DF = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
B3S_list = list(B3S_DF['name'])

data = pd.read_csv('TpsTheoriques.csv', delimiter=';')

data['KiKo'] = data.apply(lambda row: str(row['KI'])+str(row['KO']), axis=1)

data['B3S'] = data.apply(lambda row: 'N/A' if row['B3S'] == 'N/A' else "B3S" + str(row['B3S']), axis=1)

# print(data)

for index, row in data.iterrows():
    tempB3S = row["B3S"]
    list_columns = pd.read_sql_query("pragma table_info({b3s});".format(b3s=tempB3S), conn)
    commit = True


    if len(list_columns) == 34:
        cur.execute("ALTER TABLE {b3s} ADD COLUMN TpsTheoriques;".format(b3s=tempB3S))

    elif len(list_columns) == 0:
        commit = False
        continue

    if commit and str(row["Temps théoriques"]) != 'nan':
        cur.execute('''UPDATE {b3s} SET TpsTheoriques =  {TpsTh} 
            WHERE Type_Message IS '{PointGestion}'
            AND NumLigne IS '{Ligne}' 
            AND KiKo IS '{KiKo}' 
            AND Sens IS '{Dir}' '''.format(b3s=row["B3S"], TpsTh=row["Temps théoriques"],
                                           PointGestion=row["Point de gestion"],
                                           Ligne=row["Ligne"], KiKo=row["KiKo"], Dir=row["Direction"]))
    conn.commit()

conn.close()