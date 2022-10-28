#! /usr/bin/env python3
# coding: utf-8

""" A compact data profiling tool by PuffInc https://github.com/puffinc
free use and reuse as long as you state Puffinc as th original author """

import pandas as pd
import traceback as tb
from itertools import combinations
import re
from tqdm import tqdm
import math

import plotly.express as px
import plotly.offline as pof
import plotly.graph_objs as go
from plotly.subplots import make_subplots

import phik
from phik.report import plot_correlation_matrix
from phik import report


def main():
    """ Main data profiling part"""
    df = import_csv_dataset("centrales-de-production-nucleaire-et-thermique-a-flamme-edf.csv",';')
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
                          'has_trailing_spaces_regex_example':"regex_not_working_yet"}
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

        bool_regex = regex_checker(values,'^[0-1]|True|true|False|false*$')
        date_regex = regex_checker(values,'^[0-3]?[0-9]/[0-3]?[0-9]/(?:[0-9]{2})?[0-9]{2}$')
        int_regex = regex_checker(values,'^[0-9]*$')
        decimal_regex = regex_checker(values,'^[-,.0-9]*$') #old deimal check rule '\d*(\.\d+)?$'
        one_word_no_accent_regex = regex_checker(values,'^[a-zA-Z]*$')
        one_word_alphanum_regex = regex_checker(values,'^[A-Za-z0-9À-ÖØ-öø-ÿ]*$') # À-ÖØ-öø-ÿ accepts accentuation but not including [ ] ^ \ × ÷
        text_regex = regex_checker(values,'^[A-Za-z0-9À-ÿ◘\/\.,;:!?()"%\-\s]*$') #looks trivial but it's not : it catches very special caracters like €&²

        regex_analysis = {'name': column,
                          'bool_regex':bool_regex[0],'b_regex_no_match':bool_regex[1],
                          'regex_a_verifier_date_dd/mm/yyyy_regex':date_regex[0],'date_regex_no_match':date_regex[1],
                          'int_no_space_regex':int_regex[0],'i_regex_no_match':int_regex[1],
                          'decimal_regex':decimal_regex[0],'d_regex_no_match':decimal_regex[1],
                          'one_word_no_accent_regex':one_word_no_accent_regex[0],'l_regex_no_match':one_word_no_accent_regex[1],
                          'one_word_alphanum_regex':one_word_alphanum_regex[0],'w_regex_no_match':one_word_alphanum_regex[1],
                          'text_regex':text_regex[0],'t_regex_no_match':text_regex[1]                      
                          }
        regexes.append(regex_analysis)
    result = pd.DataFrame(regexes)
    
    return result


def business_key_profiling(df:pd.DataFrame(),max_key_len:int):

    all_columns = df.columns.values.tolist()
    nullable_columns = df.columns[df.isna().any()].tolist()
    print('\nNullable columns are : ' + str(nullable_columns))

    keys = []
    alternate_keys = []
    potential_keys = []

    #combinaisons de toutes les clefs de toutes les longueurs (clef composée de 1 à nombre_de_col composantes)
    # jusqu'à max max_key_len qui est déjà trop de possibilités
    for i in range(1, min(len(all_columns),max_key_len) + 1):
        combs = combinations(all_columns, i)
        for potential_key in combs : potential_keys.append(potential_key)

    for potential_key in tqdm(potential_keys):
        # check if the column or a subset of col is nullable 
        # (if so it is not a key : all parts of a key are not-nullable by definition)
        a_subkey_is_nullable = False
        for nullable_column in nullable_columns:
            if nullable_column in potential_key:
                a_subkey_is_nullable = True
                subkey = nullable_column

        if a_subkey_is_nullable:
            is_key = False
            example = 'A subset or a column of the key is a nullable : ' + str(subkey)
        else:
            #check if a subset is a key (auquel cas le set actuel est forcément key)
            a_subkey_is_already_a_key = False
            for alternate_key in alternate_keys:
                if set(alternate_key).issubset(set(potential_key)):
                    a_subkey_is_already_a_key = True
                    subkey = alternate_key

            if a_subkey_is_already_a_key:
                is_key = False
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

        key_analysis = {'key': str(list(potential_key)), 'is_key':is_key, 'Raison de non clef : doublon/subkey':example }
        keys.append(key_analysis)
    result = pd.DataFrame(keys)

    return result


def null_profiling(df:pd.DataFrame()):
    nulls = []
    for column in df:
        col_len = len(df[column])
        null_amount = df[column].isnull().sum()
        null_percentage = round((null_amount/col_len)*100,2)
        column_null_analysis = {'name': column, 'len':col_len, 'nulls': null_amount, 'null_%':null_percentage}
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
    html = result.to_html(classes = 'table table-dark table-responsive-sm table-sm table-hover', index=False, table_id='my_table')
    html = '<BR>' + title + '<BR>' + html

    return html


# def init_html_results():
#     """ creates a string concatenating the boostrap references"""
#     css = '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">'
#     js1 = '<script src="https://code.jquery.com/jquery-3.3.1.slim.min.js" integrity="sha384-q8i/X+965DzO0rT7abK41JStQIAqVgRVzpbzo5smXKp4YfRvH+8abtTE1Pi6jizo" crossorigin="anonymous"></script>'
#     js2 = '<script src="https://cdn.jsdelivr.net/npm/popper.js@1.14.7/dist/umd/popper.min.js" integrity="sha384-UO2eT0CpHqdSJQ6hJty5KVphtPhzWj9WO1clHTMGa3JDZwrnQq4sF86dIHNDz0W1" crossorigin="anonymous"></script>'
#     js3 = '<script src="https://cdn.jsdelivr.net/npm/bootstrap@4.3.1/dist/js/bootstrap.min.js" integrity="sha384-JjSmVgyd0p3pXB1rRibZUAYoIIy6OrQ6VrjIEaFf/nJGzIxFDsf4x0xIM+B07jRM" crossorigin="anonymous"></script>'
    
#     inited_html = css + js1 + js2 + js3 + '<BR>'
    
#     return inited_html


def correlation_analysis(df: pd.DataFrame()):
    phik_overview = df.phik_matrix()
    phik_overview.round(2)
    print(phik_overview)


def plotly_build_barplot(df:pd.DataFrame, x_column:str, y_column:str,z_column:str, barplot_title:str, barplot_orientation='h'):
    """ build a generic staked barplot diagram into html
    x_column : data for x axis
    y column : data for y axis (measured data : must be an int)
    z column : stacked measured data"""

    fig = px.bar(df,
                 x=x_column,
                 y=y_column,
                 color=z_column,
                 title=barplot_title,
                 color_continuous_scale='OrRd',
                 orientation=barplot_orientation)

    html_heatmap = pof.plot(fig, include_plotlyjs=True, output_type='div')

    return html_heatmap


def pie_charting_ordinal_and_binary_values(df:pd.DataFrame,values_analysis_results:pd.DataFrame):
    # list of all the Binary or Ordinal cols (i.e. having less than 10 different values)
    ordinal_and_binary_analysis_df = values_analysis_results[(values_analysis_results['type'] == 'ordinal')|(values_analysis_results['type'] == 'binary')]
    print(ordinal_and_binary_analysis_df)
    ordinal_and_binary_df_col_list = ordinal_and_binary_analysis_df['name'].tolist()
    print(ordinal_and_binary_df_col_list)
    ordinal_and_binary_col_amount = len(ordinal_and_binary_df_col_list)

    # payload df of only ordinary and binary values
    df_binary_or_ordinal = df[ordinal_and_binary_df_col_list]
    print(df_binary_or_ordinal)

    # building the grid specifications (specs) and titles
    rows = round(ordinal_and_binary_col_amount/2)
    cols = 2
    specs = [[{'type':'domain'}] * cols] * rows
    subplot_titles = ordinal_and_binary_df_col_list

    # building the subplots canevas based on the grid specs
    fig = make_subplots(
            rows=rows,
            cols=cols,
            specs=specs,
            subplot_titles=subplot_titles,
            print_grid=True) # prints the resulting grid coordinates as stdout :"This is the format of your plot grid:[ (1,1)  ]  [ (1,2)  ]"

    #build individual data and pie chart par column
    i = 0
    for column in ordinal_and_binary_df_col_list:
        i = i + 1
        #
        this_col_values = df_binary_or_ordinal.groupby([column]).agg(count = pd.NamedAgg(column =column, aggfunc= 'count'))
        this_col_values['value'] = this_col_values.index

        print(this_col_values)

        # compute target position
        # if impair col = 1 else col = 2
        if (i % 2) == 0:
            col_for_this_chart = 2
        else:
            col_for_this_chart = 1

        # row computation (/2 car car deux pie par row, arrondi au sup)
        row_for_this_chart = math.ceil(i/2)

        # populating the canevas position (1,1) with pie charts
        fig.add_trace(go.Pie(labels=this_col_values['value'],
                            values=this_col_values['count'],
                            showlegend=False,
                            textposition='inside',
                            textinfo='label+value'), #Any combination of ['label', 'text', 'value', 'percent'] joined with '+' characters (e.g. 'label+text')
                    row=row_for_this_chart,
                    col=col_for_this_chart)
    
    fig.update_layout(title="Binary & Ordinal Values analysis", title_x=0.5)

    # output
    html_result = pof.plot(fig, include_plotlyjs=True, output_type='div')

    return html_result

#28/10/22 brute force conditionnal color ; TODO abstarct method

def binary_colorization(cell_value):
    if cell_value == 'binary': highlight = 'background-color: #CFF800;'
    else: highlight = ''
    return highlight
def ordinal_colorization(cell_value):
    if cell_value == 'ordinal': highlight = 'background-color: #00B0BA;'
    else: highlight = ''
    return highlight
def continuous_colorization(cell_value):
    if cell_value == 'continuous': highlight = 'background-color: #FFEC59;'
    else: highlight = ''
    return highlight
def true_colorization(cell_value):
    if ((cell_value == 'True') or (cell_value == True)): highlight = 'background-color: #4DD091;'
    else: highlight = ''
    return highlight
def false_colorization(cell_value):
    if ((cell_value == 'False') or (cell_value == False)): highlight = 'background-color: #FF6F68;'
    else: highlight = ''
    return highlight


def profile_dataset(df: pd.DataFrame(), output_html_file: str):
    """ Dataprofiling pipeline """
    
    # Set CSS properties for th elements in dataframe
    th_props = [('font-size', '14px'),
                ('text-align', 'center'),
                ('font-weight', 'bold'),
                ('color', 'white'),
                ('background-color', '#383E42'),
                ('font-family', 'Arial')]

    # Set CSS properties for td elements in dataframe
    td_props = [('font-size', '12px'),
                ('font-family', 'Arial'),
                ('border-bottom','2px solid lavender')]

    # Set table styles
    styles = [dict(selector="th", props=th_props),
    dict(selector="td", props=td_props)]

    # 28/10/2022 : test de décommisionner bootsrap et d'utiliser directement df.style
    # html_results = init_html_results()
    html_results = ''

    # !!!!!! ci dessus a ajouter dans la parstie HTMLisée !!!!!!!!
    print(f"\ndf.shape() : {(df.shape)[1]} colonnes ; {(df.shape)[0]} lignes (hors entete)")
    html_result = "<BR>Data shape (df.Shape()) :"
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
    html_result = html_result + "<BR> The dataset has " + str(columns_amount) + " columns, of which " + str(distinct_col_amount) + " are distinct<BR>"
    html_results = html_results + html_result

    # df.head(10)
    result = df.head(10)
    result = result.astype('str')
    styled_result = result.style.set_table_styles(styles)
    html_result = result_output(styled_result,'df.head(10)')
    html_results = html_results + html_result

    # Missing value analysis
    result = null_profiling(df)
    styled_result = result.style.background_gradient(subset=["nulls"]).bar(subset=["null_%"]).format(precision=2, subset=["null_%"]).set_table_styles(styles)
    html_result = result_output(styled_result,'Missing value analysis')
    html_results = html_results + html_result
    # barchart hereunder less useful is barcahrt already appearing as a style in the above table
    # html_result_graph = plotly_build_barplot(result,'null_%','name','nulls','Null analysis','h')
    # html_results = html_results + html_result_graph

    # df.describe() numeric cols
    result = df.describe(include='number').T
    # le nom de la colonne passe en index, on la remet comme col en 1ere position
    result['Attribut'] = result.index
    first_column = result.pop('Attribut')
    result.insert(0, 'Attribut', first_column)
    result = result.reset_index()
    #graphic design
    styled_result = result.style.background_gradient(subset=['count']).format(precision=0).set_table_styles(styles)
    # export
    html_result = result_output(styled_result,'df.describe() numeric cols')
    html_results = html_results + html_result

    #df.describe() non numeric cols
    result = df.describe(exclude='number').T
    # le nom de la colonne passe en index, on la remet comme col en 1ere position
    result['Attribut'] = result.index
    first_column = result.pop('Attribut')
    result.insert(0, 'Attribut', first_column)
    result = result.reset_index()
    #graphic design
    styled_result = result.style.background_gradient(subset=['count']).background_gradient(subset=['unique']).background_gradient(subset=['freq']).set_table_styles(styles)
    # export
    html_result = result_output(styled_result,'df.describe() non numeric cols')
    html_results = html_results + html_result

    # Values profiling
    result = values_profiling(df)
    styled_result = result.style.applymap(binary_colorization).applymap(ordinal_colorization).applymap(continuous_colorization).set_table_styles(styles)
    html_result = result_output(styled_result,'Values profiling')
    html_results = html_results + html_result
    html_result_graph = pie_charting_ordinal_and_binary_values(df,result)
    html_results = html_results + html_result_graph
    # TODO : replace plein de pie chart par un barchart :
    # |
    # |    nuke               EDF
    # |    thermique          EDF
    # |____thermique__________EDF____________
    #       Type              proprio
    # print(result)
    # html_result_graph2 = plotly_build_barplot(result)
    # html_results = html_results + html_result_graph2

    # Whitespace analysis"
    result = whitespace_profiling(df)
    styled_result = result.style.set_table_styles(styles)
    html_result = result_output(styled_result,'Whitespace analysis')
    html_results = html_results + html_result

    #Regex matcher (for not null cols) - after trimming whitespaces and excluding null values within columns ")
    # df is trimmed since trailing/leading whitespace make regex profiling less interesting 
    # (and because most of the time data is trimmed before use)
    trimed_df = df.copy() # else df and trimed_df points toward the same in_memory references hence modifying df instead of only trimed_df
    trimed_df = trim(trimed_df)
    result = regex_profiling(trimed_df)
    styled_result = result.style.applymap(true_colorization).applymap(false_colorization).set_table_styles(styles)
    html_result = result_output(styled_result,'Regex matcher (for not null cols) - after trimming whitespaces and excluding null values withon columns.')
    html_results = html_results + html_result

    #Recherche des clefs métier
    max_key_length = 3
    result = business_key_profiling(df,max_key_length)
    title = 'Clef retenues [paramétré pour chercher les combinaisons d\'une longueur max de ' + str(max_key_length) +' colonnes]'
    keys_df = result[result['is_key'] == True]
    if len(keys_df) == 0:
        html_result = 'no PK found '
    else:
        styled_result = keys_df.style.applymap(true_colorization).applymap(false_colorization).set_table_styles(styles)
        html_result = result_output(styled_result,title)
    html_results = html_results + html_result
    title = 'Recherche des clefs métier [paramétré pour chercher les combinaisons d\'une longueur max de ' + str(max_key_length) +' colonnes]'
    styled_result = result.style.applymap(true_colorization).applymap(false_colorization).set_table_styles(styles)
    html_result = result_output(styled_result,title)
    html_results = html_results + html_result

    # Correlation analysis using phi(k)
    # to do : comprendre comment ça marche pour le rendre raisonnablement executable en temps
    # correlation_analysis(df)

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