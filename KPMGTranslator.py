# -*- coding: utf-8 -*-
"""
Created on Fri May 12 12:52:37 2023

@author: salonydubey
"""

import logging
import os
import sys
from argparse import ArgumentParser
from sql_translate import translation
from sql_translate import Validator
# from sql_translate.lineage import extract_objects
from datetime import datetime
import uuid
import pandas as pd 
from sql_translate import kpmg_create_schema
from sql_translate.lineage import create_lineage
from sql_translate.translatefromViews import create_select_statements


# logging = logging.getlogging(__name__)

ImpalaToPresto = translation.ImpalaToPresto()

logging.basicConfig(filename='translation.log', level=logging.INFO, format='%(asctime)s -  %(levelname)s -%(message)s',force=True)

def read_from_folder(filename, directory):
    if not filename.endswith(".txt"):
        raise ValueError("Please provide a TEXT file")

    filepath = os.path.join(str(directory), filename)
    logging.info("filepath: %s", filepath)

    with open(filepath, "r") as f:
        query = f.read()

    return query, filename


def read_from_file(inputfilepath):
    if not inputfilepath.endswith(".txt"):
        raise ValueError("Please provide a TEXT file")

    with open(inputfilepath, "r") as f:
        query = f.read()

    return query


def write_output_in_folder(outputpath, filename, translatedquery):
    if not os.path.exists(outputpath):
        os.makedirs(outputpath)

    complete_name = os.path.join(outputpath, f"{filename}")
    with open(complete_name, "w") as f:
        f.write(translatedquery)

    logging.info("File successfully saved at: %s", complete_name)


def translate_query(inputquery, has_insert_statement=False):
    logging.info("input query read : %s", inputquery)
    translated_query = ImpalaToPresto.translate_statement(inputquery, has_insert_statement)
    logging.info("Translated query : %s", translated_query)

    return translated_query


def print_output(inputquery, outputquery):
    print("*" * 100)
    print("Impala Input Query: %s", inputquery)
    # print("*" * 100)
    print("")
    print("Presto Output Query: %s", outputquery)
    print("*" * 100)


def process_folder(folder_path, output_folder_path=None):
    df = pd.DataFrame()
    data ={'executionId':[],
           'file':[],
            'query' :[],
            'translated_query':[]}
    output_folder_path = folder_path + "_output" if output_folder_path is None else output_folder_path

    if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
        raise ValueError(f"{folder_path} is not a valid folder path.")

    for file in os.listdir(folder_path):
        try:
            query, filename = read_from_folder(file, folder_path)
            data['query'].append(query)
            data['file'].append(filename)


            executionId = uuid.uuid5(uuid.NAMESPACE_URL,query).hex
            data['executionId'].append(executionId)
            logging.info(" Execution Id %s", executionId)
            
            translated_query = translate_query(query)
            data['translated_query'].append(translated_query)

            write_output_in_folder(output_folder_path, filename, translated_query)
            print_output(query, translated_query)
            # return query, translated_query
        except Exception as e:
            logging.exception(e)
    df = pd.DataFrame.from_dict(data)
    return df



def process_query(query):
    translated_query = translate_query(query)
    print_output(query, translated_query)


def process_text_file(text_file_path):
    query = read_from_file(text_file_path)
    translated_query = translate_query(query)
    print_output(query, translated_query)


def getdata (filepath1, filepath2):
    file1 = pd.read_excel(filepath1)
    file2 = pd.read_excel(filepath2)
    data = pd.concat([file1,file2],ignore_index=True)
    return data




def main():
    logging.basicConfig(level=logging.INFO)
    starttime =  datetime.now()
    logging.info("Process started: %s", starttime)

    print("Process Started :",starttime)
    
    # process_query("select chr(97)")
    
    # data = process_folder(r'C:\Users\salonydubey\OneDrive - KPMG\translator\input', r'C:\Users\salonydubey\OneDrive - KPMG\translator\input_output')
    # df = extract_objects.extract_objects(data)
    # Validator.validate(data,'','')
    # Create an argument parser to handle command-line arguments
    parser = ArgumentParser()
    parser.add_argument("-f", "--folder", type=str, help="Loop files from a folder")
    parser.add_argument("-o", "--output", type=str, help="The folder to save processed files")
    parser.add_argument("-q", "--query", help="Take query as input")
    parser.add_argument("-t", "--translator", help="Take list of view as input")
    parser.add_argument("-m", "--mappingpath", help="Take list of view and its corresponding new view names(mapped data)  as input")

    


    parser.add_argument('-v', "--Yes", help='Run validation.py')
    parser.add_argument('-i', '--impalafilepath', help='provide input path of impala select queries')
    parser.add_argument('-p', '--prestofilepath', help='provide input path of presto select queries')


    parser.add_argument('-ih', '--impala_hostname', help='impala host name')
    parser.add_argument('-ip', '--impala_port', help='impala port number')
    
    parser.add_argument('-ph', '--presto_hostname', help='presto hostname')
    parser.add_argument('-pp', '--presto_port', help='presto port number')
    parser.add_argument('-pc', '--catalog', help='presto catalog')
    parser.add_argument('-ps', '--schema', help='presto schema')
    parser.add_argument('-s', '--create_schema', help='create schema')

    parser.add_argument('-prefix', '--prefix', help='takes prefix to create new view list')
    parser.add_argument('-suffix', '--suffix', help='takes suffix to create new view list')


    parser.add_argument('-l', '--lineage', help='presto schema')
    parser.add_argument('-vl', '--viewlist', help='presto schema')
    parser.add_argument('-tl', '--tablelist', help='presto schema')

    # Parse the arguments passed through the command-line
    args = parser.parse_args()
    
    if (args.lineage != None ) and (args.create_schema!=None) and (args.translator != None) :
            data = pd.read_excel(args.viewlist)#or cvs to do
            lineage_data = create_lineage.get_lineage(data)
            prefix = args.prefix
            suffix = args.suffix
            kpmg_create_schema.create_presto_statements(lineage_data, prefix, suffix)
            df = create_select_statements(lineage_data, prefix, suffix)
        #     if  (args.impala_hostname !=None)  and (args.impala_port!= None) :
        #         impalaconnection = {
        #             "host" : args.impala_hostname,
        #             "port" : args.impala_port,
        #         }
        #         print(impalaconnection)
        #     else:
        #         print("With validation mode please provide database credentials \nfor help pleae use -h")
        #         exit(1)

        #     # prestoconnection = args.prestoconnection.split(" ")
        #     if  (args.presto_hostname !=None)  and (args.presto_port!= None) and (args.catalog!= None) :
        #         prestoconnection = {
        #             "host": args.presto_hostname,
        #             "port" :args.presto_port,
        #             "catalog" :args.catalog,
        #             "schema" :args.schema
        #             }
        #         print(prestoconnection)

        #     else:
        #             print("With validation mode please provide database credentials")
        #             exit(1)
        #    # Run validator 
        #     Validator.validate(df, impalaconnection, prestoconnection)
        
        
    # if (args.lineage != None ) and (args.create_schema!=None) and (args.translator  is None) and (args.Yes is None):
    #         data = pd.read_excel(args.viewlist)#or cvs to do
    #         lineage_data = create_lineage.get_lineage(data)
    #         kpmg_create_schema.create_presto_statements(lineage_data)
    
    if (args.lineage != None  and args.create_schema is None) or (args.lineage is None  and args.create_schema != None):

        if args.lineage :
                print("lineage code will run")
                data = pd.read_excel(args.viewlist)
                create_lineage.get_lineage(data)
                
        if args.create_schema:
                # print("schema code will run")    
                if (args.mappingpath) and (args.prefix is None) and (args.suffix is None):
                    data = pd.read_excel(args.mappingpath)#to do read csv also
                    kpmg_create_schema.create_presto_statements(data)
                    
                if args.prefix:
                    prefix= args.prefix
                    data = pd.read_excel(args.mappingpath)
                    # get_prefix_or_suffix(data,args.prefix)
                    kpmg_create_schema.create_presto_statements(data, prefix )
                    
                if args.suffix:
                        suffix= args.suffix
                        data = pd.read_excel(args.mappingpath)
                        # get_prefix_or_suffix(data,args.prefix)
                        kpmg_create_schema.create_presto_statements(data,None, suffix)
                # df = extract_objects.extract_objects(data,schema=True,lineage=False)
            
    # Check if the '-t' argument is passed 
    if args.translator:
        try:
            if (args.mappingpath) and (args.prefix is None) and (args.suffix is None):
                data1 = pd.read_excel(args.mappingpath)
                df = create_select_statements(data1)
            if args.prefix:
                prefix= args.prefix
                data1 = pd.read_excel(args.mappingpath)
                df = create_select_statements(data1,prefix)
            if args.suffix:
                suffix= args.suffix
                data1 = pd.read_excel(args.mappingpath)
                df = create_select_statements(data1,None,suffix)

            if args.Yes =='Yes':
                    if  (args.impala_hostname !=None)  and (args.impala_port!= None) :
                        impalaconnection = {
                            "host" : args.impala_hostname,
                            "port" : args.impala_port,
                        }
                        print(impalaconnection)
                    else:
                        print("With validation mode please provide database credentials \nfor help pleae use -h")
                        exit(1)

                    # prestoconnection = args.prestoconnection.split(" ")
                    if  (args.presto_hostname !=None)  and (args.presto_port!= None) and (args.catalog!= None) :
                        prestoconnection = {
                            "host": args.presto_hostname,
                            "port" :args.presto_port,
                            "catalog" :args.catalog,
                            "schema" :args.schema
                            }
                        print(prestoconnection)

                    else:
                            print("With validation mode please provide database credentials")
                            exit(1)

                    Validator.validate(df, impalaconnection, prestoconnection)
            # process_text_file(args.textfile)
        except Exception as e:
                logging.exception(e)
                    

    # Check if the '-f' argument is passed
    if args.folder:
        data = process_folder(args.folder, args.output)
        if args.Yes =='Yes':
                if  (args.impala_hostname !=None)  and (args.impala_port!= None) :
                    impalaconnection = {
                        "host" : args.impala_hostname,
                        "port" : args.impala_port,
                    }
                    print(impalaconnection)
                else:
                    print("With validation mode please provide database credentials \nfor help pleae use -h")
                    exit(1)

                # prestoconnection = args.prestoconnection.split(" ")
                if  (args.presto_hostname !=None)  and (args.presto_port!= None) and (args.catalog!= None) :
                    prestoconnection = {
                        "host": args.presto_hostname,
                        "port" :args.presto_port,
                        "catalog" :args.catalog,
                        "schema" :args.schema
                        }
                    print(prestoconnection)

                else:
                        print("With validation mode please provide database credentials")
                        exit(1)

                Validator.validate(data, impalaconnection, prestoconnection)
        else:
           print("Validation mode diabled. To enable Validation Please Say Yes.")
          
           
    # run validation if -v as 'Yes'  is provided in CMD 
    if args.Yes =='Yes':
            if (args.impalafilepath != None ) and (args.prestofilepath !=None):
                if not os.path.exists(args.impalafilepath) or not os.path.isdir(args.prestofilepath):
                    raise ValueError(f"{args.impalafilepath} is not a valid folder path.")
                
                data ={     'file':[],
                            'query' :[],
                            'translated_query':[],
                            'executionId' :[]}
                for file in os.listdir(args.impalafilepath):
    
                    try :
                        impala_query, filename = read_from_folder(file, args.impalafilepath)
                        presto_query, filename = read_from_folder(file, args.prestofilepath)
                        executionId = ''
                        data['query'].append(impala_query)
                        data['executionId'].append(executionId)
                        data['file'].append(filename)
                        data['translated_query'].append(presto_query)
                        
                        
                    except Exception as e :
                        logging.exception(e)
                    
                df = pd.DataFrame.from_dict(data)
                # print(df)
                
                if  (args.impala_hostname !=None)  and (args.impala_port!= None) :
                    impalaconnection = {
                        "host" : args.impala_hostname,
                        "port" : args.impala_port,
                    }
                    print(impalaconnection)
                else:
                    print("With validation mode please provide database credentials \nfor help pleae use -h")
                    exit(1)
       
                # prestoconnection = args.prestoconnection.split(" ")
                if  (args.presto_hostname !=None)  and (args.presto_port!= None) and (args.catalog!= None) :
                    prestoconnection = {
                        "host": args.presto_hostname,
                        "port" :args.presto_port,
                        "catalog" :args.catalog,
                        "schema" :args.schema
                        }
                    print(prestoconnection)
       
                else:
                        print("With validation mode please provide database credentials")
                        exit(1)
       
                Validator.validate(df, impalaconnection, prestoconnection)
            else:
                print('To run validation independently please provide impala query path and presto query path')
                logging.exception('To run validation independently please provide impala query path and presto query path')
                exit(1)
    else:
        print("Validation mode diabled. To enable Validation Please Say Yes.")
                   

   # Check if the '-q' argument is passed
    if args.query:
        try:
            process_query(args.query)
        except Exception as e:
            logging.exception(e)


                

                
    Endtime =  datetime.now()
    logging.info("Process Ended: %s", Endtime)
    print("Process Ended :",Endtime)
    print("Total Time taken",Endtime  - starttime )

            

if __name__ =="__main__":
    main()
    
