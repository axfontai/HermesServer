import pandas as pd
import sqlite3 as sql

# dir dans lequel est installé la database sqlite3
SQLiteDir = '/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux/SQlite'

# se connecte se la base de donnee sqlite3 Hermes
conn = sql.connect(SQLiteDir + '/Hermes3.db')
cur = conn.cursor()

# liste des b3s dans la base de donnée
B3S_DF = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
B3S_list = list(B3S_DF['name'])


for b3s in B3S_list:
    """
    sql = "DELETE FROM {b3s} WHERE Jour NOT BETWEEN '2017-07-01' AND '2018-01-01'; ".format(b3s=b3s)
    cur.execute(sql)
    print(sql)
    """
    ListNomCarrefours = pd.read_sql_query('SELECT DISTINCT NomCarrefour FROM ' + b3s + ';', conn)
    ListNomCarrefours = ListNomCarrefours['NomCarrefour'].tolist()

    if len(ListNomCarrefours) > 1:
        ListNumCarrefours = pd.read_sql_query('SELECT DISTINCT NumCarrefour FROM ' + b3s + ';', conn)
        ListNumCarrefours = ListNumCarrefours['NumCarrefour'].tolist()
        for carrefour in ListNumCarrefours:
            if int(carrefour[0]) >=9:
                cur.execute("SELECT NomCarrefour FROM {b3s} WHERE NumCarrefour={carrefour} LIMIT 1;"
                            .format(b3s=b3s, carrefour=carrefour))
                NomCarrefour = cur.fetchall()[0][0]
                print(NomCarrefour)
                NewB3S = "B3S"+carrefour
                SqliteTable = ''' CREATE TABLE IF NOT EXISTS {} 
                                                            ( Jour text, NumCarrefour text, 
                                                            NomCarrefour text, NumLigne text, 
                                                            Sens text, H_VA text, X_VA real, 
                                                            Y_VA real, P_VA integer, 
                                                            H_AA text, X_AA real, 
                                                            Y_AA real, P_AA integer, 
                                                            H_LF text, X_LF real, 
                                                            Y_LF real, H_AF text, 
                                                            X_AF real, Y_AF real, 
                                                            P_AF integer, TpsFranchissement text, 
                                                            KiKo text, TpsArret_VA_AA text, TpsArret_AA_LF text,
                                                            Prio text, NumParc text, WeekDay text,
                                                            TpsVA_AA integer, DistVA_AA real, SpeedVA_AA real,
                                                            TpsAA_LF integer, DistAA_LF real, SpeedAA_LF real, 
                                                            TpsLF_AF integer, DistLF_AF real, SpeedLF_AF real,
                                                            TpsVA_AF integer, SpeedVA_AF real, 
                                                            X_VA_merc real null, Y_VA_merc real null,
                                                            X_AA_merc real null, Y_AA_merc real null,
                                                            X_LF_merc real null, Y_LF_merc real null, 
                                                            X_AF_merc real null, Y_AF_merc  real null
                                                            ) ; '''.format(NewB3S)
                cur.execute(SqliteTable)
                cur.execute(
                    "INSERT INTO {Newb3s} SELECT * FROM {CurrentB3S} WHERE NomCarrefour={CarrefourDifferent};".format(
                        Newb3s=NewB3S, CurrentB3S=b3s, CarrefourDifferent="'" + NomCarrefour + "'"))
                cur.execute("DELETE FROM {B3S} WHERE NomCarrefour={CarrefourDifferent};".format(
                    B3S=b3s, CarrefourDifferent="'" + NomCarrefour + "'"))
    conn.commit()

#cur.execute("VACUUM;")

conn.close()