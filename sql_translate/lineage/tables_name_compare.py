import pandas as pd

def compare_table_names(df):
    try:
        # Read table names from text files
        file1_path=r'I:\Coral\sql_translate\sql_translate\tables_list_client.txt'
        with open(file1_path, 'r') as file1:
            table_lst = set([line.strip() for line in file1.readlines()])
        file2_path=r'I:\Coral\sql_translate\sql_translate\views_list_client.txt'
        with open(file2_path, 'r') as file2:
            view_lst = set([line.strip() for line in file2.readlines()])

        
        # df = pd.read_csv('df.csv')
        # table_names_dataframe = set(df['object'])
       
        # Create an empty DataFrame to store the results
        result_df = pd.DataFrame(columns=['Object', 'Type', 'Label'])
        # Iterate over the dataframe rows
        for index, row in df.iterrows():
            obj = row['Object']
            obj_type = row['Type']
            
            if obj_type == 'View':
                if obj in view_lst:
                    result_df = pd.concat([result_df, pd.DataFrame({'Object': [obj], 'Type': ['view'], 'Label': ['client']})])
                else:
                    result_df = pd.concat([result_df, pd.DataFrame({'Object': [obj], 'Type': ['view'], 'Label': ['tool']})])
            elif obj_type == 'Table':
                if obj in table_lst:
                    result_df = pd.concat([result_df, pd.DataFrame({'Object': [obj], 'Type': ['table'], 'Label': ['client']})])
                else:
                    result_df = pd.concat([result_df, pd.DataFrame({'Object': [obj], 'Type': ['table'], 'Label': ['tool']})])
        



        # Save the dataframe to a CSV file

        final_output = result_df.drop_duplicates(subset="Object")
        final_result = final_output.to_csv('unique_table_names.csv', index=False)
        print("Unique table names saved to 'unique_table_names.csv'")

    except FileNotFoundError as e:
        print(f"File not found: {e.filename}")
    except IOError as e:
        print(f"Error reading file: {e.filename}")




# df = 'df.csv'  # Replace with the path
# file1_path = 'file1.txt'  # Replace with the path to the first text file
# file2_path = 'file2.txt'  # Replace with the path to the second text file

# compare_table_names(df)
