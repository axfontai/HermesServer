import os
import sqlite3
import csv
import pandas as pd
import math
import pyproj

# cree une Base de Données formatée a partir des CSV extraits de excel

os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu = os.getcwd()
DirCSV: str = DirTestCarrefoursFeu + "/MeldpuntenCSV"
DirSQlite = DirTestCarrefoursFeu + "/SQLite"
# prends les fichier ci dessus comme fichiers de travail

list_in_hermes = []

# Connect to SQLite database
connection = sqlite3.connect(DirSQlite + "/Meldpunten.db")
c = connection.cursor()

tempB3S = ''


def lambert72_to_wgs84(lon, lat):
    # transforme les coordonnées lambert72 en WGS84
    x, y = pyproj.transform(pyproj.Proj(init='epsg:31370'), pyproj.Proj(init='epsg:4326'), lon, lat)
    return x, y


def wgs84_to_webmercator(lon, lat):
    # transforme les coordonnées WGS84 en webmercator
    x, y = pyproj.transform(pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:3857'), lon, lat)
    return x, y


def dist_from_coord(x1, y1, x2, y2):
    # calcule la distance en metres entre deux coordonnees WGS84
    R = 6371000
    phi_1 = math.radians(y1)
    phi_2 = math.radians(y2)
    delta_phi = math.radians(y2 - y1)
    delta_lambda = math.radians(x2 - x1)
    a = math.sin(delta_phi / 2.0) ** 2 + \
        math.cos(phi_1) * math.cos(phi_2) * \
        math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    dist = c * R
    return dist


def delta_t(t2, t1):
    if t1 > t2:
        return 30
    elif t1 == t2:
        return 30
    else:
        return (pd.to_datetime(t2) - pd.to_datetime(t1)).seconds


Commit = True
for CSVfile in os.listdir(DirCSV):
    # pour tout les fichiers CSV dans le répertoire
    print(CSVfile)
    date = CSVfile[:2] + '-' + CSVfile[2:4] + '-' + CSVfile[4:8]
    if CSVfile == ".DS_Store":
        continue
    # Sur Mac chaque fichier contient un DS_Store qui ne peut être lu
    elif CSVfile not in list_in_hermes:
        with open(DirCSV + "/" + CSVfile) as CurrentCSV:
            # Pour chaque fichier csv dans le répertoire
            reader = csv.reader(CurrentCSV, delimiter=";")
            ligne = 1
            for i, row in enumerate(reader):
                # se connecte a la base de données
                if i == ligne:
                    tempB3S = "B3S" + str(int(row[0]))
                    SqliteTable = ''' CREATE TABLE IF NOT EXISTS {} 
                                            (B3S_Recalcule text null, KiKo text null, 
                                            NumLigne text null, Type_Message text null,
                                            NomCarrefour text null, Cle text null,
                                            CF_Actif text null, CF_Test text null,
                                            LF_Actif null, Priorite text null, Mode text null,
                                            Sens text null, Iti_Actif text null, 
                                            From_Num_Arret text null, From_Nom_Arret text null,
                                            To_Num_Arret text null, To_Nom_Arret text null,
                                            Distance_LF integer null, Type_Gestion text null, 
                                            Power text null, Ordre text null,
                                            X_PG_lambert real null, Y_PG_lambert real null, 
                                            X_PG_wgs84 real null, Y_PG_wgs84 real null, 
                                            X_PG_webmerc real null, Y_PG_webmerc real null, 
                                            X_effectif_lambert real null, Y_effectif_lambert real null,
                                            X_effectif_wgs84 real null, Y_effectif_wgs84 real null,
                                            X_effectif_webmerc real null, Y_effectif_webmerc real null,
                                            date text null
                                            ) ; '''.format(tempB3S)
                    c.execute(SqliteTable)
                    RowValues = ''
                    RowValuesList = []
                    ListNotAdd = [0, 3, 10, 11, 23, 24, 28, 30]
                    ListXYtoWGS84 = [27, 29]  # Colonnes a formatter en WGS84
                    # ListY = [28, 30]
                    ListCoordMerc = []
                    for j in range(30):

                        # formatte les colonnes du CSV
                        if j in ListXYtoWGS84:
                            if row[j] == 'N/A'or row[j+1] == 'N/A':
                                for k in range(6):
                                    RowValuesList.append('null')
                            else:
                                x_wgs, y_wgs = lambert72_to_wgs84(float(row[j]), float(row[j + 1]))
                                x_merc, y_merc = wgs84_to_webmercator(x_wgs, y_wgs)
                                RowValuesList += float(row[j]), float(row[j + 1]), x_wgs, y_wgs, x_merc, y_merc
                        elif j in ListNotAdd:
                            continue
                        elif row[j] == 'N/A':
                            RowValuesList.append('null')
                        elif j == 2:
                            RowValuesList.append(str(str(row[j]) + str(row[j + 1])))
                        elif j == 21:
                            RowValuesList.append(int(row[j]))
                        else:
                            if isinstance(row[j], str) and "'" in row[j]:
                                RowValuesList.append(row[j].replace("'", " "))
                            elif row[j][-2:] == '.0':
                                RowValuesList.append(str(int(float(row[j]))))
                            else:
                                RowValuesList.append(str(row[j]))
                    if Commit:  # Si il n'y a pas de valeurs vides,
                        RowValuesList.append(date)
                        for k in RowValuesList:
                            if isinstance(k, str):
                                RowValues += "'" + k + "'" + ','
                            else:
                                RowValues += str(k) + ','
                        RowValues = RowValues[:len(RowValues) - 1]
                        print(RowValues)
                        insertion = 'INSERT INTO {} VALUES '.format(tempB3S) + '(' + RowValues + ')'
                        c.execute(insertion)
                        connection.commit()
                    Commit = True
                    ligne += 1

    else:

        continue

# print(in_hermes_test)
# print(list_in_hermes)

connection.commit()
connection.close()
