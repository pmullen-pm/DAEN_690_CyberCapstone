# -*- coding: utf-8 -*-
"""
Created on Wed Apr 05 20:32:10 2017

@author: John
"""


from csv import reader, writer

def load_csv(filename):
    myfile = open(filename, "rt")
    lines = reader(myfile)
    dataset = list()    
    #this avoids any empty rows
    for row in lines:
        if not row:
            continue
        dataset.append(row) 
    #dataset = list(lines) #good if you know dataset has no empty rows
    myfile.close()
    return dataset 

def transform_dataset(dataset):        
    dark = []
    columnNames = []
    for row in dataset:
        columnIndex = 0
        #print row
        if row[0] != "ip":
           
            lastScore_greater_5 = int(float(row[4]))
            if lastScore_greater_5 < 5:
                lastScore_greater_5 = "lastScore_greater_5_false"
            else:
                lastScore_greater_5 = "lastScore_greater_5_true"         
            
            
            columnIndex = 11
            thisRowsCustomerList =[]
            for columnIndex in range(11,98):
                #handle the customer column case
                #print columnIndex
                if(int(float(row[columnIndex])) > 0):
                    #print columnIndex
                    #print columnNames
                    thisRowsCustomerList.append(columnIndex)               
                    
                    
            
            dark.append((lastScore_greater_5,) + tuple(thisRowsCustomerList))    
        else:
            #this is the first row only i hope
            for column in range(11,88):
                columnNames.append(row[column])
    return dark

def write_csv(transformed_dataset):    
    with open('d3_community5.csv','wb') as fp: #new file name
        a = writer(fp, delimiter=",")
        #a.writerow(['ip', 'avgScore_greater_5', 'trendUp', 'trendDown', 'trueCount', 'mostCommonCustomerHit', 'totalCustomersHit'])
        a.writerows(transformed_dataset) #writes each row of data to new file
        print "file written"

#load the data
filename = 'full_no5s.csv'
community = load_csv(filename)
print ('Loaded data file {0} with {1} rows and {2} columns').format(filename, len(community), len(community[0]))

#transform the data and save to new list structure
new1 = transform_dataset(community)

#write new csv file from list 
make_csv_file = write_csv(new1)