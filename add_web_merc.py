import os
import sqlite3
import pandas as pd
import pyproj


#prends les fichier ci dessous comme fichiers de travail
os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu = os.getcwd()
DirCSV: str = DirTestCarrefoursFeu+"/CSV"
DirSQlite = DirTestCarrefoursFeu+"/SQLite"

#Connect to SQLite database
connection = sqlite3.connect(DirSQlite+"/Hermes3.db")

c = connection.cursor()
c2 = connection.cursor()
#liste des b3s dans la base de donnée
B3S_DF = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", connection)
B3S_list = list(B3S_DF['name'])
print(B3S_list)

def wgs84_to_webmercator(lon, lat):
    #transforme les coordonnées WGS84 en webmercator
    x, y = pyproj.transform(pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:3857'), lon, lat)
    return x, y

for b3s in B3S_list:

    #cree les colonnes
    c.execute("ALTER TABLE {B3S} ADD COLUMN X_VA_merc REAL NULL;".format(B3S=b3s))
    c.execute("ALTER TABLE {B3S} ADD COLUMN Y_VA_merc REAL NULL;".format(B3S=b3s))

    c.execute("ALTER TABLE {B3S} ADD COLUMN X_AA_merc REAL NULL;".format(B3S=b3s))
    c.execute("ALTER TABLE {B3S} ADD COLUMN Y_AA_merc REAL NULL;".format(B3S=b3s))

    c.execute("ALTER TABLE {B3S} ADD COLUMN X_LF_merc REAL NULL;".format(B3S=b3s))
    c.execute("ALTER TABLE {B3S} ADD COLUMN Y_LF_merc REAL NULL;".format(B3S=b3s))

    c.execute("ALTER TABLE {B3S} ADD COLUMN X_AF_merc REAL NULL;".format(B3S=b3s))
    c.execute("ALTER TABLE {B3S} ADD COLUMN Y_AF_merc REAL NULL;".format(B3S=b3s))

    #update les colonnes
    c.execute('SELECT * FROM {B3S};'.format(B3S=b3s))
    i = 1
    for row in c:
        c2.execute("UPDATE {B3S} SET X_VA_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[6],row[7])[0], id=i))
        c2.execute("UPDATE {B3S} SET Y_VA_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[6],row[7])[1], id=i))

        c2.execute("UPDATE {B3S} SET X_AA_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[10],row[11])[0], id=i))
        c2.execute("UPDATE {B3S} SET Y_AA_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[10],row[11])[1], id=i))

        c2.execute("UPDATE {B3S} SET X_LF_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[14],row[15])[0], id=i))
        c2.execute("UPDATE {B3S} SET Y_LF_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[14],row[15])[1], id=i))

        c2.execute("UPDATE {B3S} SET X_AF_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[17],row[18])[0], id=i))
        c2.execute("UPDATE {B3S} SET Y_AF_merc = {newcoord} WHERE rowid={id};".format(B3S=b3s, newcoord=wgs84_to_webmercator(row[17],row[18])[1], id=i))
        i += 1
    print(b3s)

connection.commit()
connection.close()
