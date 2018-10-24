import os
import sqlite3
import csv
import pandas as pd
import math
import pyproj

#cree une Base de Données formatée a partir des CSV extraits de excel

os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu = os.getcwd()
DirCSV: str = DirTestCarrefoursFeu+"/CSV"
DirSQlite = DirTestCarrefoursFeu+"/SQLite"
#prends les fichier ci dessus comme fichiers de travail

list_in_hermes = []

with open('InHermes', 'r') as in_hermes_test:
    in_hermes = in_hermes_test.read()
    list_in_hermes = in_hermes.split(';')

connection = sqlite3.connect(DirSQlite+"/Hermes3.db")
c = connection.cursor()
#Connect to SQLite database

tempB3S = ''
def DateTimeToWeekday(row):
#cree une colone avec le jour de la semaine au moment de l'evenement
    WeekDay = row.weekday()
    if WeekDay == 0:
        return 'Lun'
    elif WeekDay == 1:
        return 'Mar'
    elif WeekDay == 2:
        return 'Mer'
    elif WeekDay == 3:
        return 'Jeu'
    elif WeekDay == 4:
        return 'Ven'
    elif WeekDay == 5:
        return 'Sam'
    elif WeekDay == 6:
        return 'Dim'
    else:
        return 'Else'

def lambert72_to_wgs84(lon, lat):
    #transforme les coordonnées lambert72 en WGS84
    x, y = pyproj.transform(pyproj.Proj(init='epsg:31370'), pyproj.Proj(init='epsg:4326'), lon, lat)
    return x, y

def wgs84_to_webmercator(lon, lat):
    #transforme les coordonnées WGS84 en webmercator
    x, y = pyproj.transform(pyproj.Proj(init='epsg:4326'), pyproj.Proj(init='epsg:3857'), lon, lat)
    return x, y

def dist_from_coord(x1, y1, x2, y2):
    #calcule la distance en metres entre deux coordonnees WGS84
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
    if t1> t2:
        return 30
    elif t1==t2:
        return 30
    else:
        return (pd.to_datetime(t2) - pd.to_datetime(t1)).seconds

Commit = True
for CSVfile in os.listdir(DirCSV):
    #pour tout les fichiers CSV dans le répertoire
    print(CSVfile)
    if CSVfile == ".DS_Store":
        continue
    #Sur Mac chaque fichier contient un DS_Store qui ne peut être lu
    elif CSVfile not in list_in_hermes:
        with open(DirCSV + "/" + CSVfile) as CurrentCSV:
        # Pour chaque fichier csv dans le répertoire
            reader = csv.reader(CurrentCSV, delimiter=";")
            ligne = 2
            for i, row in enumerate(reader):
                #se connecte a la base de données
                if i == ligne:
                    tempB3S = "B3S" + str(int(row[1][-5:-2]))
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
                                            ) ; '''.format(tempB3S)
                    c.execute(SqliteTable)
                    RowValues = ''
                    RowValuesList = []
                    ListFltoTxt = [1, 3, 21, 24, 25]
                    ListIntToInt = [8, 12, 19] #Colonnes a formatter en Int
                    ListXYtoWGS84 = [6, 10, 14, 17] #Colonnes a formatter en WGS84
                    ListY = [7, 11, 15, 18]
                    ListCoordMerc = []
                    for j in range(26):
                        #formatte les colonnes du CSV
                        if row[j] == 'N/A':
                            Commit = False
                            break
                        elif j in ListY:
                            continue
                        elif j in ListFltoTxt:
                            RowValuesList.append(str(row[j][:-2]))
                        elif j == 0:
                            date = str(str(row[0][:11]) + str(row[5]))
                            RowValuesList.append(date)
                        elif j in ListIntToInt:
                            RowValuesList += row[j]
                        elif j in ListXYtoWGS84:
                            #print(row[j], row[j+1])
                            x, y = lambert72_to_wgs84(float(row[j]), float(row[j+1]))
                            x_merc, y_merc = wgs84_to_webmercator(x, y)
                            ListCoordMerc.append(x_merc)
                            ListCoordMerc.append(y_merc)
                            RowValuesList += x, y
                        else:
                            RowValuesList.append(str(row[j]))
                    if Commit: #Si il n'y a pas de valeurs vides,
                        # ajoute des nouvelles colonnes dans la DB
                        RowValuesList.append(DateTimeToWeekday(pd.to_datetime(row[0])))# ajoute le jour de la semaine
                        # ajoute les temps - Distances - et vitesses entre messages
                        TpsVA_AA = delta_t(row[9],row[5])
                        DistVA_AA = dist_from_coord(RowValuesList[6], RowValuesList[7], RowValuesList[10], RowValuesList[11])
                        SpeedVA_AA = (DistVA_AA/TpsVA_AA) * 3.6
                        RowValuesList += TpsVA_AA, DistVA_AA, SpeedVA_AA

                        TpsAA_LF = delta_t(row[13],row[9])
                        DistAA_LF = dist_from_coord(RowValuesList[10], RowValuesList[11], RowValuesList[14], RowValuesList[15])
                        SpeedAA_LF = (DistAA_LF / TpsAA_LF) * 3.6
                        RowValuesList += TpsAA_LF, DistAA_LF, SpeedAA_LF

                        TpsLF_AF = delta_t(row[16],row[13])
                        DistLF_AF = dist_from_coord(RowValuesList[14], RowValuesList[15], RowValuesList[17], RowValuesList[18])
                        SpeedLF_AF = (DistAA_LF / TpsAA_LF) * 3.6
                        RowValuesList += TpsLF_AF, DistLF_AF, SpeedLF_AF

                        TpsVA_AF = TpsVA_AA + TpsAA_LF + TpsLF_AF
                        SpeedVA_AF = ((DistVA_AA + DistAA_LF + DistLF_AF)/TpsVA_AF) * 3.6

                        RowValuesList += TpsVA_AF, SpeedVA_AF
                        RowValuesList += ListCoordMerc

                        for k in RowValuesList:
                            if isinstance(k,str):
                                RowValues += "'" + k + "'" + ','
                            else:
                                RowValues += str(k) + ','
                        RowValues = RowValues[:len(RowValues) - 1]
                        #print(RowValues)
                        insertion = 'INSERT INTO {} VALUES '.format(tempB3S) + '(' + RowValues + ')'
                        c.execute(insertion)
                        connection.commit()
                    Commit = True
                    ligne += 1
        list_in_hermes.append(CSVfile)

    else:

        continue
list_in_hermes_string = ''

for j in list_in_hermes:
    list_in_hermes_string += j + ';'

with open('InHermes', 'w') as in_hermes_test:
    in_hermes_test.write(list_in_hermes_string)

#print(in_hermes_test)
#print(list_in_hermes)

connection.commit()
connection.close()
