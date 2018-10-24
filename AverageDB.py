import pandas as pd
import pandas.io.sql as pdsql
import sqlite3 as sql
import numpy as np

# dir dans lequel est installé la database sqlite3
SQLiteDir = '/Volumes/Données/Python/TestCarrefoursFeux/SQlite'
# /Users/griceldacalzada/Documents/Python/TestCarrefoursFeux/SQlite

# se connecte se la base de donnee sqlite3 Hermes qui contient les donnees individuelles
# et à la db Meldpunten qui contient les donnees theoriques
hermes_conn = sql.connect(SQLiteDir + '/Hermes3.db')
average_conn = sql.connect(SQLiteDir + '/HermesAverage.db')

# liste des b3s dans la base de donnée
B3S_DF = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
B3S_list = list(B3S_DF['name'])

