import sqlite3
import re
import pandas as pd
import numpy as np
import datetime
from configparser import ConfigParser

def deleteHistory(cur,conn,tableName,whereCond):
    try:
        deleteQuery="delete from "+tableName+whereCond
        # print(deleteQuery)
        cur.execute(deleteQuery)
        result = cur.rowcount
        conn.commit()
        return result
    except Exception as e:
        print("Error in deleting browsing data from db")
        print(e)