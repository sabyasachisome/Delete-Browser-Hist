from FetchData import fetchData as fetch
from TransactData import transactions as transact
from sys import exit

if __name__=="__main__":
    try:
#         get connection object
        configObj=fetch.getConfig()
        con,cur=fetch.getConnection(configObj)
        
#         get table and where condition
        table,whereCond=fetch.getDbDetails(configObj)
#         print(table,whereCond)

#         get the data from table
        df1=fetch.readData(con,table,whereCond)
        if len(df1.index)==0:
            exit('Dataframe is empty')
        else:
            # df1.head(2)

            df2=fetch.convertColumns(df1,'last_visit_time')
            # df2.head(4)

#             refiltering the data, in case data pulled in df2 was not filtered, for safety
            # df3=fetch.filterData(df2,configObj)
            # df3.head(2)
            # df3[df3['url'].str.contains('FACEBOOK',case=False, regex=True)]
            
#             deleting records finally
            recordsDeleted= transact.deleteHistory(cur,con,table,whereCond)
            print(recordsDeleted," records have been deleted")
            
    except Exception as e:
        print("Error in execution")
        print(e)