#! /usr/bin/env python3
# coding: utf-8

""" A compact data profiling tool by PuffInc https://github.com/puffinc
free use and reuse as long as you state Puffinc as th original author """

import pandas as pd
import traceback as tb
from itertools import combinations
import re
from tqdm import tqdm


def main():
    """ Main data profiling part"""
    df = import_csv_dataset("commune_2022.csv",',')
    #df = import_excel_dataset("test.xlsx")

    profile_dataset(df,'Hercules_report.html')


def import_csv_dataset(csv:str,separateur=";", encodage='utf-8'):
    """ Import data for profiling"""
    # possible encoding 'utf-8'
    df = pd.read_csv(csv,
                     sep=separateur,
                     encoding=encodage,
                     skiprows=None)
    return df


def import_excel_dataset(file_path:str, header_row=1, skip_first_rows=0):
    """ Import data for profiling"""
    df = pd.read_excel(file_path,
                       header=header_row,
                       skiprows=skip_first_rows)
    return df


def regex_match_finder(column:pd.DataFrame(),regex_pattern:str):
    """ Returns a values matching a regex,
    note : used to check if AT LEAST ONE values match (e.g. has leading whitespace)
    and not to check that ALL the values match"""

    match_example = '[No match]'
    Regex = re.compile(regex_pattern)

    if len(column) == 0:
        match_example = '[Empty col]'
    else:   
        for value in column:
            result = Regex.match(str(value))
            if result is not None :
                match_example = value
    
    return match_example


def regex_checker(column:pd.DataFrame(),regex_pattern:str):
    """ checks if ALL the values of the column matches the regex_pattern,
    returns a boolean match/no match
    and if no match an example not matching the regex_pattern """
    match = True
    no_match_example = '[No no-match]'
    Regex = re.compile(regex_pattern)

    if len(column) == 0:
        no_match_example = '[Empty col]'
    else:   
        for value in column:
            result = Regex.match(str(value))
            if result is None:
                match = False
                no_match_example = value

    return match,no_match_example


def whitespace_profiling(df:pd.DataFrame()):
    whitespace_regexes = []
    for column in df:
        # drop np.nan et 'nan' post-typage à 'string'
        values = df[column].dropna()
        values = values[values != 'nan']

        has_leading_spaces_regex = regex_match_finder(values,'^\s+')
        has_trailing_spaces_regex = regex_match_finder(values,'^\s+$')

        regex_analysis = {'name': column,
                          'has_leading_spaces_regex_example':has_leading_spaces_regex,
                          'has_trailing_spaces_regex_example':"not_working_yet"}
                          #'has_trailing_spaces_regex_example':has_trailing_spaces_regex}
        whitespace_regexes.append(regex_analysis)
    result = pd.DataFrame(whitespace_regexes)

    return result


def regex_profiling(df: pd.DataFrame()):
    regexes = []
    for column in df:
        # drop np.nan et 'nan' post-typage à 'string'
        values = df[column].dropna()
        values = values[values != 'nan']

        int_regex = regex_checker(values,'^[0-9]*$')
        decimal_regex = regex_checker(values,'^[-,.0-9]*$') #old deimal check rule '\d*(\.\d+)?$'
        one_word_no_accent_regex = regex_checker(values,'^[a-zA-Z]*$')
        one_word_alphanum_regex = regex_checker(values,'^[A-Za-z0-9À-ÖØ-öø-ÿ]*$') # À-ÖØ-öø-ÿ accepts accentuation but not including [ ] ^ \ × ÷
        text_regex = regex_checker(values,'^[A-Za-z0-9À-ÿ◘\/\.,;:!?()"%\-\s]*$') #looks trivial but it's not : it catches very special caracters like €&²

        regex_analysis = {'name': column,
                          'int_no_space_regex':int_regex[0],'i_regex_no_match':int_regex[1],
                          'decimal_regex':decimal_regex[0],'d_regex_no_match':decimal_regex[1],
                          'one_word_no_accent_regex':one_word_no_accent_regex[0],'l_regex_no_match':one_word_no_accent_regex[1],
                          'one_word_alphanum_regex':one_word_alphanum_regex[0],'one_word_alphanum_regex':one_word_alphanum_regex[1],
                          'text_regex':text_regex[0],'t_regex_no_match':text_regex[1]                      
                          }
        regexes.append(regex_analysis)
    result = pd.DataFrame(regexes)
    
    return result


def business_key_profiling(df:pd.DataFrame(),max_key_len:int):
    #TODO : ajouter une exclusion des colonnes nullables des clefs pontentielles "a subkey is nullable"
    all_columns = df.columns.values.tolist()
    nullable_columns = df.columns[df.isna().any()].tolist() #https://stackoverflow.com/questions/36226083/how-to-find-which-columns-contain-any-nan-value-in-pandas-dataframe
    print("work to be done here :: exclusion of nullable columns")
    print(str(nullable_columns))

    keys = []
    alternate_keys = []
    potential_keys = []

    #combinaisons de toutes les clefs de toutes les longueurs (clef composée de 1 à nombre_de_col composantes)
    # jusqu'à max max_key_len qui est déjà trop de possibilités
    for i in range(1, min(len(all_columns),max_key_len) + 1):
        combs = combinations(all_columns, i)
        for potential_key in combs : potential_keys.append(potential_key)

    for potential_key in tqdm(potential_keys):
        #check if a subset is a key (auquel cas le set actuel est forcément key)
        a_subkey_is_already_a_key = False
        for alternate_key in alternate_keys :
            if set(alternate_key).issubset(set(potential_key)):
                a_subkey_is_already_a_key = True
                subkey = alternate_key

        if a_subkey_is_already_a_key: 
            is_key = True
            alternate_keys.append(potential_key)
            example = 'A subset is a key : ' + str(subkey)
        else:
            keyed_df = df[list(potential_key)]
            if len(keyed_df) == len(keyed_df.drop_duplicates()):
                is_key = True
                alternate_keys.append(potential_key)
                example = ''
            else:
                is_key = False
                duplicate_row = keyed_df[keyed_df.duplicated(keep=False)]
                example_line = str(duplicate_row.head(1).index.values.tolist()[0] + 1 )
                example_value = str(duplicate_row.head(1).values.tolist()[0])
                example = 'ligne ' + example_line + ' : ' + example_value
                
        key_analysis = {'key': str(list(potential_key)), 'is_key':is_key , 'Raison de non clef : doublon/subkey':example }
        keys.append(key_analysis)
    result = pd.DataFrame(keys)
    
    return result


def null_profiling(df:pd.DataFrame()):
    nulls = []
    for column in df:
        col_len = len(df[column])
        null_amount = df[column].isnull().sum()
        null_percentage = round((null_amount/col_len)*100,2)
        column_null_analysis = {'name': column,'len':col_len ,'nulls': null_amount,'null_%':null_percentage}
        nulls.append(column_null_analysis)
    result = pd.DataFrame(nulls)

    return result


def values_profiling(df:pd.DataFrame()):
    values = []
    for column in df:
        distinct_values = df[column].drop_duplicates().values.tolist()
        if len(distinct_values) <= 2:
            values_type = 'binary'
            values_sample = distinct_values
        elif len(distinct_values) <= 10:
            values_type = 'ordinal'
            values_sample = distinct_values
        else :
            values_type = 'continuous'
            values_sample = distinct_values[:10]

        values_analysis = {'name': column,'type': values_type ,'samples': values_sample}
        values.append(values_analysis)
    result = pd.DataFrame(values)
    
    return result


def trim(df:pd.DataFrame()):
    """ removes all leading and trailing whitespace
    !!! all datatypes are changed as string during the process"""
    for col in df:
        df[col] = df[col].values.astype('str')
        df[col] = df[col].str.strip()
    return df


def result_output(result:pd.DataFrame(),title:str):
    """ prints the result, and prepare an html output"""
    
    #output console
    print('\n' + title)
    print(result)
    
    #html output preparation (generation of an html string)
    html = result.to_html(classes = 'table table-dark table-responsive-sm table-sm table-hover',index = False, table_id = 'my_table')
    html = '<BR>' + title + '<BR>' + html
    
    return html
    

def init_html_results():
    """ creates a string concatenating the boostrap references"""
    css = '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">'
    js1 = '<script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>'
    js2 = '<script src="https://cdn.jsdelivr.net/npm/popper.js@1.14.7/dist/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>'
    js3 = '<script src="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>'
    
    inited_html = css + js1 + js2 + js3 + '<BR>'
    
    return inited_html
    

def profile_dataset(df:pd.DataFrame(),output_html_file:str):
    """ Dataprofiling pipeline """
    
    html_results = init_html_results()
    
    # !!!!!! ci dessus a ajouter dans la parstie HTMLisée !!!!!!!! 
    print(f"\ndf.shape() : {(df.shape)[1]} colonnes ; {(df.shape)[0]} lignes (hors entete)")
    html_result = "<BR>df.shape() :"
    html_result = html_result + "<BR>df.shape : " + str((df.shape)[1]) + " columns"
    html_result = html_result + "<BR>df.shape : " + str((df.shape)[0]) + " lignes (hors entete)<BR>"
    html_results = html_results + html_result
    
    print("\nCheck that there is no duplicated col name")
    columns_amount = len(df.columns.values.tolist())
    distinct_col_amount = len(set(df.columns.values.tolist()))
    print(f" The dataset has {columns_amount} columns, of which {distinct_col_amount} are distinct")
    if columns_amount != distinct_col_amount:
        print("!!!!!!!! WARNING - duplicated column name : the analysis will be KO ")
    html_result = "<BR>Check that there is no duplicated col name :"
    html_result = html_result + "<BR> The dataset has " + str(columns_amount) + " columns, of which " + str(distinct_col_amount) + "are distinct<BR>"
    html_results = html_results + html_result

    #df.head(10)
    result = df.head(10)
    html_result = result_output(result,'df.head(10)')
    html_results = html_results + html_result
    
    #Missing value analysis
    result = null_profiling(df)
    html_result = result_output(result,'Missing value analysis')
    html_results = html_results + html_result
    
    #df.describe() numeric cols
    result = df.describe(include='number').T
    # le nom de la colonne passe en index, on la remet comme col en 1ere position
    result['Attribut'] = result.index
    first_column = result.pop('Attribut')
    result.insert(0, 'Attribut', first_column)
    # export
    html_result = result_output(result,'df.describe() numeric cols')
    html_results = html_results + html_result
    
    #df.describe() non numeric cols
    result = df.describe(exclude='number').T
    # le nom de la colonne passe en index, on la remet comme col en 1ere position
    result['Attribut'] = result.index
    first_column = result.pop('Attribut')
    result.insert(0, 'Attribut', first_column)
    # export
    html_result = result_output(result,'df.describe() non numeric cols')
    html_results = html_results + html_result
    
    # Values profiling
    result = values_profiling(df)
    html_result = result_output(result,'Values profiling')
    html_results = html_results + html_result
    
    # Whitespace analysis"
    result = whitespace_profiling(df)
    html_result = result_output(result,'Whitespace analysis')
    html_results = html_results + html_result
    
    #Regex matcher (for not null cols) - after trimming whitespaces and excluding null values withon columns ")
    # df is trimmed since trailing/leadin whitespace make regex profiling less interesting 
    # (and because most of the time data is trimmed before use)
    trimed_df = trim(df)
    result = regex_profiling(trimed_df)
    html_result = result_output(result,'Regex matcher (for not null cols) - after trimming whitespaces and excluding null values withon columns.')
    html_results = html_results + html_result

    #Recherche des clefs métier
    result = business_key_profiling(df,3)
    html_result = result_output(result,'Recherche des clefs métier')
    html_results = html_results + html_result
    
    # output as html 
    html_file=open(output_html_file,"w")
    html_file.write(html_results)
    html_file.close
        
try:
    if __name__ == "__main__":
        main()

except Exception as error:
    print("Exception \n") 
    print("\nerror type {0}\n".format(type(error)))
    print("\nerror {0}\n".format(error))
    tb.print_tb(error.__traceback__)