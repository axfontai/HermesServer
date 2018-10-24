import os
import pandas as pd


os.chdir("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux")
DirTestCarrefoursFeu=os.getcwd()


for filename in os.listdir(DirTestCarrefoursFeu+"/ExcelTemp"):
    if filename==".DS_Store" or (filename[:len(filename)-5]+".csv" in os.listdir(DirTestCarrefoursFeu+"/CSV")):
        continue
    else:

        data_xls=pd.read_excel("./ExcelTemp/"+filename,'Journal',index=None)
        data_xls.to_csv("./CSV/"+filename[:len(filename)-5]+".csv",sep=";",na_rep='N/A',index=True,encoding='UTF-8',decimal=',')























