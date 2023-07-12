from impala.dbapi import connect
from tabulate import tabulate
from sql_metadata import Parser
import pandas as pd
import logging
import json

conf = open(r'./sql_translate/lineage/lineage_config.json')
data = json.load(conf)
conn = connect(host=data['host'],port=data['port'],database=data['database'])
cursor = conn.cursor()

lineage_table=[]

def get_lineage_view(entity_name, lineage_table):
    try:
        cursor.execute(f"DESCRIBE FORMATTED {entity_name}")
        result = cursor.fetchall()
        
        dependencies = []
        for row in result:
            if row[0].startswith('View Original Text'):
                logging.info(f"{entity_name} found in impala")
                dependency = Parser(row[1]).tables                           # [default.emp,defult.emp2,default.v2]
                table_names =  [i.split('.')[1] for i in dependency]         # [emp, emp2, v2]
                dependencies=table_names       
        
        for dependency in dependencies:
            get_lineage_view(dependency,lineage_table)
            lineage_table.append([entity_name,dependency])
    except:
        logging.exception(f'{entity_name} not found in impala')
            


def get_lineage(df):
    logging.info("x--------------------Starting Lineage Module--------------------x")
    for i in df.index:
        get_lineage_view(df['View Name'][i],lineage_table)
             

    new_lt = {}
    for i in lineage_table:
        if i[0] in new_lt and i[1] not in new_lt[i[0]] :
            new_lt[i[0]].append(i[1])
        else:
            new_lt[i[0]]=[i[1]]
            

    final_result = []                      # dict is converted to lists of lists so that it can be converted to a dataframe
    for i in new_lt:
        final_result.append([i])
                
        for j in new_lt[i]:
            final_result[-1].append(j)
            

    t_and_v = set()                        # to get the unique list of tables and views...including all dependencies
    for i in final_result:
        for j in i:
            t_and_v.add(j)
                
    tables_and_views = list(t_and_v)         
    tables_and_views_df = pd.DataFrame(tables_and_views,columns=['impala object name'])        # to be passed to create statements py

    tables_and_views_type = []                                    # to add a column Type...view or table 
    for object in tables_and_views:
        cursor.execute(f"SHOW CREATE TABLE {object}")
        result = cursor.fetchall()
        result_string = result[0][0]

        if result_string.find('TABLE')!=-1:
            tables_and_views_type.append('table')
        else:
            tables_and_views_type.append('view')
    
    tables_and_views_df['type'] = tables_and_views_type

    max_length = 0
    for i in final_result:
        max_length = max(max_length, len(i))

    for i in final_result: 
        for j in range(max_length - len(i)):
            i.append('N/A')

    max_no_of_dependencies = max_length - 1 

    column_names=['View Name']
    for i in range(max_no_of_dependencies):
        column_names.append("D {}".format(i+1))
        

    df=pd.DataFrame(data=final_result,columns=column_names)
    print(df)

    df.to_excel(r'lineage.xlsx',index=False)
    tables_and_views_df.to_excel(r'impala_data_objects.xlsx',index=False)
    logging.info("x--------------------Lineage Module Ended--------------------x")
    return tables_and_views_df 