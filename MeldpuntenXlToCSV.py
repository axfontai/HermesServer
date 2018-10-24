import os
import pandas as pd


os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu=os.getcwd()


for filename in os.listdir(DirTestCarrefoursFeu+"/MeldpuntenXl"):
    if filename == ".DS_Store" or (filename[:len(filename)-5]+".csv" in os.listdir(DirTestCarrefoursFeu+"/MeldpuntenCSV")):
        continue
    else:

        data_xls = pd.read_excel("./MeldpuntenXl/"+filename, index=None)
        data_xls.to_csv("./MeldpuntenCSV/"+filename[:len(filename)-4]+".csv", sep=";", na_rep='N/A', index=False, encoding='UTF-8', decimal='.')























