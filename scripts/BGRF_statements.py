'''
Extracts thematic statements from the BGRF, maps them to concepts and prepares statements for feeding into MiMoTextbase.

Input:
- BGRF file (TSV)
- name of BGRF column from which information is to be extracted
- mapping file (TSV): defines matching between BGRF keywords and vocabulary items
'''


# =======================
# Imports
# =======================

import numpy
import pandas as pd

# =======================
# Files and folders
# =======================

bgrf_file = "BGRF_2000-100.tsv"
#bgrf_file = "BGRF_100.tsv"

bgrf_mapping = "mapping_bgrf_konvok_intentionality.tsv"

bgrf_column = "r_s"

resultfile = "bgrf2000-100_intentionality_statements.tsv"
#resultfile = "bgrf100_intentionality_statements.tsv"


# =======================
# Functions
# =======================

def read_csv(csv_file):
    '''
    Liest CSV ein und gibt sie als pandas dataframe zurück.
    '''
    with open(csv_file, "r", encoding="utf8") as infile:
        matrix = pd.read_csv(infile, sep="\t")
        return matrix

def read_csv2(csv_file, index_col_name):
    '''
    Liest CSV ein und gibt sie als pandas dataframe zurück.
    Nimmt zusätzlich Argument für die Index-Spalte
    '''
    with open(csv_file, "r", encoding="utf8") as infile:
        matrix = pd.read_csv(infile, sep="\t", index_col = index_col_name)
        return matrix

def get_keywords(mapping_matrix):
    '''
    - Takes mapping matrix.
    - Creates a dictionary with a mapping from each concept string
      to a vocabulary concept (mapping_dict).
    '''
    
    mapping_dict = {}
    for index, row in mapping_matrix.iterrows():
        list_of_keywords = []
        variants = (row['Varianten'])

        variants = variants.split("; ")
            
        for variant in variants:
            mapping_dict[variant.strip()] = row['Konzept']
            list_of_keywords.append(variant.strip())
            if row['Konzept'].strip() not in list_of_keywords:
                list_of_keywords.append(row['Konzept'])
                mapping_dict[row['Konzept'].strip()] = row['Konzept']
                
    print(mapping_dict)
    print(list_of_keywords)
    return(mapping_dict)
     

def extract_from_column(key, concept, ind, bgrf_matrix, rows, mapping_dict, bgrf_column):
    '''
    For a specific keyword column from the BGRF matrix (bgrf_column):
    mapping information is written into a dictionary.
    '''

    if key in (bgrf_matrix[bgrf_column][ind]):
        if mapping_dict[key] not in concept:
            rows.append({'id' : ind, 'Spalte' : bgrf_column, 'text' : bgrf_matrix[bgrf_column][ind], 'Keyword': key, 'property': 'tonality', 'Konzept': mapping_dict[key]})
            concept.append(mapping_dict[key])


    return rows, concept



def search_strings(bgrf_matrix, mapping_dict, bgrf_column):
    '''
    Loop over rows of the BGRF matrix and search for concept strings
    in defined bgrf column.
    With extract_from_column() rows are created that are
    joined together to form a dataframe.
    '''
    rows = []
    for ind in bgrf_matrix.index:
        concept = []
        for key in mapping_dict:   # check every concept variant
            try:
                rows, concept = extract_from_column(key, concept, ind, bgrf_matrix, rows, mapping_dict, bgrf_column)
            except:
                pass
           
    df = pd.DataFrame(rows)
    print(df)
    return df
    


def save2tsv(df, resultfile):
    '''
    Saves dataframe to TSV.
    '''
    df = df.set_index('id')
    print(df.head())
    with open(resultfile, "w", encoding="utf8") as outfile: 
        df.to_csv(outfile, index_label ="id", sep="\t", line_terminator='\n')
        


# =======================
# Coordinating function
# =======================

def main(bgrf_file, bgrf_mapping, bgrf_column, resultfile):
    bgrf_matrix = read_csv2(bgrf_file, "id")
    mapping_matrix = read_csv(bgrf_mapping)
    #print(bgrf_matrix)
    #print(mapping_matrix)
    mapping_dict = get_keywords(mapping_matrix)
    df = search_strings(bgrf_matrix, mapping_dict, bgrf_column)
    save2tsv(df, resultfile)
    
main(bgrf_file, bgrf_mapping, bgrf_column, resultfile)
    