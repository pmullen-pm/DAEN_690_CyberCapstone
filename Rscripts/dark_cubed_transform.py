# -*- coding: utf-8 -*-
"""
Created on Sat Mar 18 15:45:01 2017

@author: John
"""

import csv

myFile = open('part1_2_daen.csv','rt')
myReader = csv.reader(myFile)
dark =[]
for row in myReader:
    if row[0] != "record_id":
        
        sector = row[3]
        
        """transform size variable"""
        size=row[4]
        if size == "small":
            size = "size_small"
        elif size == "medium":
            size = "size_medium"
        else:
            size = "size_large"
        
        """transform count variable"""
        count = int(row[6])
        if (0 <= count <= 50):
            count = "seen<=50"
        elif 51 <= count <= 100: 
            count = "seen<=100"
        elif 101 <= count <= 150:
            count = "seen<= 150"
        elif 151 <= count <= 200:
            count = "seen<= 200"
        elif 201 <= count <= 250:
            count = "seen<= 250"
        elif 251 <= count <= 300:
            count = "seen<= 300"
        else:
            count  = "seen>300" 
            
        """transform score variable """
        score = int(row[9])
        if 0 <= score <= 3:
            score = "low_threat_" + str(score)
        elif 4 <= score <= 6:
            score = "mid_threat_" + str(score)
        else:
            score = "high_threat_" + str(score)
        dark.append((sector, size, count,score))       
           
myFile.close()

with open('dark_transformed.csv','wb') as fp:
    a = csv.writer(fp, delimiter=",")
    a.writerow(['sector','size','count','score'])
    a.writerows(dark)
