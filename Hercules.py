#! /usr/bin/env python3
# coding: utf-8

""" A compact data profiling tool by PuffInc
free use and reuse as long as you state Puffinc as th original author """

import pandas as pd
import traceback as tb
from itertools import combinations
import re
from tqdm import tqdm


def main():
    """ Main data profiling part"""
    df = import_dataset("commune_2022.csv")
    
    profile_dataset(df)


def import_dataset(csv:str,separateur=",", encodage='utf-8'):
    """ Import data for profiling"""
    df = pd.read_csv(csv,
                     sep=separateur,
                     encoding=encodage,
                     skiprows=None)
    return df


def regex_matcher(column:pd.DataFrame(),pattern:str):
    """ checks if the column matches the pattern, 
    returns a boolean match/no match 
    and if no match an example """    
    match = True
    example = ''
    Regex = re.compile(pattern)

    for value in column:
        result = Regex.match(str(value))
        if result is None :
            match = False
            example = value
            
    return match,example


def regex_profiling(df:pd.DataFrame()):
    regexes = []
    for column in df:
        values = df[column].dropna()
        letters_regex = regex_matcher(values,'^[a-zA-Z]*$')
        text_regex = regex_matcher(values,'^[A-Za-z0-9\.,;:!?()"%\-]*$')
        num_regex = regex_matcher(values,'^[0-9]*$')
        decimal_regex = regex_matcher(values,'\d*(\.\d+)?$')
        alphanum_regex = regex_matcher(values,'^[A-Za-z0-9]*$')
        regex_analysis = {'name': column,
                          'letters_regex':letters_regex[0],'letters_no_match':letters_regex[1],
                          'text_regex':text_regex[0],'text_no_match':text_regex[1],
                          'num_regex':num_regex[0],'num_no_match':num_regex[1],
                          'decimal_regex':decimal_regex[0],'decimal_no_match':decimal_regex[1],
                          'alphanum_regex':alphanum_regex[0],'alphanum_no_match':alphanum_regex[1]}
        regexes.append(regex_analysis)
    print(pd.DataFrame(regexes))


def business_key_profiling(df:pd.DataFrame(),max_key_len:int):
    all_columns = df.columns.values.tolist()
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
    print(pd.DataFrame(keys))
    pd.DataFrame(keys).to_html('keys.html')


def null_profiling(df:pd.DataFrame()):
    nulls = []
    for column in df:
        col_len = len(df[column])
        null_amount = df[column].isnull().sum()
        null_percentage = round((null_amount/col_len)*100,2)
        column_null_analysis = {'name': column,'len':col_len ,'nulls': null_amount,'null_%':null_percentage}
        nulls.append(column_null_analysis)
    print(pd.DataFrame(nulls))


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
    print(pd.DataFrame(values))

    
def profile_dataset(df:pd.DataFrame()):
    """ Dataprofiling pipeline """

    print(f"\ndf.shape() : {(df.shape)[1]} colonnes ; {(df.shape)[0]} lignes (hors entete)")
    
    print("\nCheck that there is no duplicated col name")
    columns_amount = len(df.columns.values.tolist())
    distinct_col_amount = len(set(df.columns.values.tolist()))
    print(f" The dataset has {columns_amount} columns, of which {distinct_col_amount} are distinct")
    if columns_amount != distinct_col_amount:
        print("!!!!!!!! WARNING - duplicated column name : the analysis will be KO ")

    print("\ndf.head(10)")
    print(df.head(10))

    print("\nMissing value analysis")
    null_profiling(df)

    print("\ndf.describe() numeric cols")
    print(df.describe(include='number').T)
    
    print("\ndf.describe() non numeric cols")
    print(df.describe(exclude='number').T)

    print("\nValues profiling")
    values_profiling(df)

    print("\nRegex matcher (for not null cols)")
    regex_profiling(df)

    print("\nRecherche des clefs métier")
    business_key_profiling(df,4)
    
try:
    if __name__ == "__main__":
        main()

except Exception as error:
    print("Exception \n") 
    print("\nerror type {0}\n".format(type(error)))
    print("\nerror {0}\n".format(error))
    tb.print_tb(error.__traceback__)

