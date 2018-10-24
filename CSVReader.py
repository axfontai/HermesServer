import csv

#CSVfile=pd.read_csv("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux/CSV/journalcarrefourfeu_2016-03-19_2016-03-20.csv")
#print(CSVfile.nr)




with open("/Users/griceldacalzada/Documents/Python/TestCarrefoursFeux/CSV/journalcarrefourfeu_2016-03-19_2016-03-20.csv",
          'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=';')
    x=22
    values=''
    ListFlToInt=[1,3,21,24,25]
    ListIntToInt=[6,7,8,10,11,12,14,15,17,18, 19]
    for i,row in enumerate(reader):
        if i==x :
            print(row)
            for j in range(26):
                if row[j]=='N/A':
                    


                elif j in ListFlToInt:
                    Nbre=row[j]
                    values+= Nbre[:len(Nbre)-2]
                    values+= ','
                elif j in ListIntToInt:
                    values+=row[j]
                    values+=','
                else:
                    values+="'"+row[j]+"'"+','

print(values[:len(values)-1])





