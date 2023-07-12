# -*- coding: utf-8 -*-
"""
Created on Tue May 30 10:51:55 2023

@author: salonydubey
"""

from impala.dbapi import connect
import pandas as pd

def check_objects(object_set, executionId):
    database= 'default'
    # Connect to Impala
    conn = connect(host='localhost', port=21050)
    cursor = conn.cursor()

    # Switch to the specified database
    cursor.execute('USE {}'.format(database))

    # Retrieve list of tables and views
    # cursor.execute('SHOW TABLES')      
    # table_list = set(row[0] for row in cursor.fetchall())


    # Retrieve list of views
    # cursor.execute('SHOW VIEWS')
    # view_list = set(row[0] for row in cursor.fetchall())
    
    results =[]
    # Check each object in the set
    for obj in object_set:
        # table_list=['store','emp']
        # view_list=['date','date_dim']
        # print(obj)
        cursor.execute(f"SHOW CREATE TABLE {obj}")
        result = cursor.fetchall()
        # print(result[0][0])
        if result[0][0].find('VIEW')!=-1:
            results.append((executionId,obj, 'View', 'Tool'))
            print('{} is a view.'.format(obj))
        elif result[0][0].find('TABLE')!=-1:
            results.append((executionId,obj, 'Table','Tool'))
            print('{} is a table.'.format(obj))


        # if obj.lower() in table_list:
        #     results.append((executionId,obj, 'Table','Tool'))
        #     # print('{} is a table.'.format(obj))
        # # elif obj in view_list:
        # #     results.append((executionId,obj, 'Table', 'Tool'))
        # #     print('{} is a view.'.format(obj))
        else:
            results.append((executionId,obj, 'Not found','Tool'))
            # print('{} is neither a table nor a view.'.format(obj))

    # print(results)
    # Close the connection
    df = pd.DataFrame(results, columns=['executionId','Object', 'Type','Label'])
    return df

