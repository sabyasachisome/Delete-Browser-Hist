import sqlite3
import re
import pandas as pd
import numpy as np
import datetime
from configparser import ConfigParser

def getConfig():
    configObj= ConfigParser()
    configObj.read('config.ini')
    return configObj

def getConnection(configObj):
    try:
        dbName=configObj.get('dbProps','dbname')
        conn=sqlite3.connect(dbName)
        cur=conn.cursor()
        return (conn,cur)
    except Exception as e:
        print("Provide correct DB path")
        print(e)

def getDbDetails(configObj):
    try:
        tableName=configObj.get('dbProps','dbTable')
        filterColName=" upper("+configObj.get('dbProps','dbTableSearchCol')+") like "
        whereCond=str(filterColName).join(["'%"+value+"%' or" for (key,value) in configObj.items('siteKeywords')])
        whereCond=" where ( "+filterColName+" "+whereCond[:-3]+")"
        return (tableName, whereCond)
    except Exception as e:
        print("Error in fetching db Details")
        print(e)
        
def readData(conn, tableName, whereCond):
    try:
        selString="select * from "+tableName+whereCond
        df=pd.read_sql(selString,conn)
        return df
    except Exception as e:
        print("Error in fetching data")
        print(e)

def convertColumns(dfName,colName):
    try:
        dfName[colName]=dfName.apply(lambda row: datetime.datetime(1601, 1, 1)+ \
                                             datetime.timedelta(microseconds=row[colName]), axis=1)
        dfName.sort_values(by=colName, inplace=True, ascending=False)
        return dfName
    except Exception as e:
        print("Error in coverting timestamp column")
        print(e)

def filterData(histDf,configObj):
    try:
        searchKeywords='|'.join([value for (key,value) in configObj.items('siteKeywords')])
        filteredDf= histDf[histDf['url'].str.contains(str(searchKeywords),case=False, regex=True)]
        return filteredDf
    except Exception as e:
        print("Error in fetching data from db")
        print(e)