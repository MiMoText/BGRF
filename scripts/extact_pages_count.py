## Imports
from os.path import join
import pandas as pd
import re

import sys
from SPARQLWrapper import SPARQLWrapper, JSON

def get_wikibase_data():
    endpoint_url = "https://query.mimotext.uni-trier.de/proxy/wdqs/bigdata/namespace/wdq/sparql"
    query = """
        prefix wd:<http://data.mimotext.uni-trier.de/entity/>
        prefix wdt:<http://data.mimotext.uni-trier.de/prop/direct/> 
        SELECT *
        WHERE 
        {
        ?item wdt:P2 wd:Q2; wdt:P22 ?bgrf; wdt:P25 ?num_pages; wdt:P26 ?distribution_format.
        }"""

    def get_results(endpoint_url, query):
        user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
            # TODO adjust user agent; see https://w.wiki/CX6
        sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()["results"]["bindings"]

    results = get_results(endpoint_url, query)
    #print(type(results))
    wikibase = pd.json_normalize(results)
    wikibase = wikibase.drop(columns=['item.type', 'num_pages.xml:lang', 'num_pages.type', 'distribution_format.xml:lang',
                     'distribution_format.type', 'bgrf.type'])
    return wikibase

def add_commata(x):
    # as some numbers are missing the separating comma, we are going to add it
    if re.search("[0-9](\s)[0-9]", x):
        found = re.search("([0-9-]+)(\s)([-0-9]+)", x)
        if len(found.group(1)) <= 2 or len(found.group(3)) <= 2:
            x = re.sub(" ", "",x)
            x = re.sub(" ", "",x)
        x = re.sub(found.group(2), ", ", x)
        x = re.sub(",,", ",", x)
    return x

def sub_chars(x):
    #print("x", x)
    try:
        x = re.sub("[^\d-]+", ",", x)
        return x
    except AttributeError:
        print("ATTRERR", x)
        return x

def add_numbers(x):
    all_nums_cleaned = []
    numbers = [n for n in x.split(",") if n]
    try:
        # everything is fine
        n = sum([int(n) for n in numbers if int(n) >= 0])
        return n
    
    except ValueError:
        # there are still - in numbers
        for i, n in enumerate(numbers):
            # the both first list objects are - => convert them to 0
            if numbers[0] == "-" and numbers[1] == "-":
                numbers[0] = 0
                numbers[1] = 0
            # only the first list object is - => convert it to 0
            if n == "-" and i == 0:     
                n = 0
            # if it is between numbers
            elif n == "-":
                number1 = int(numbers[i+1])
                number2 = int(numbers[i-1])
                all_nums_cleaned.append(number1-number2)
                del numbers[(i+1)]
                del numbers[(i)]
                del numbers[(i-1)]
            try:
                num = int(n)
                all_nums_cleaned.append(num)
            except ValueError:
                if re.search("[0-9]+-[0-9]+", n):
                    nums = [int(num) for num in n.split("-")]
                    all_nums_cleaned.append(max(nums) - min(nums))
                
                elif n[:-1] == "-":
                    diff_nums_cleaned = [int(num) if num and num != "-" else 0 for num in n.split("-") ][0]+ int(numbers[i+1])
                    all_nums_cleaned.append(diff_nums_cleaned)
            x = sum([int(n) for n in all_nums_cleaned])
        return x
    
def clean_pages(comb):
    #### to do:
    # check if a + was in pages, then if numbers before or after it are in cleaned pages, delete it, otherwise add it
    # same for roman numbers
    comb["to_check"] = ["yes" if re.search("-", x) else "no" for x in comb["num_pages.value"]]
    comb["num_pages_cleaned"] = comb["num_pages.value"].copy()
    comb["num_pages_cleaned"] = comb["num_pages_cleaned"].apply(lambda x: re.sub("I", "1", x))
    comb["num_pages_cleaned"] = comb["num_pages_cleaned"].apply(lambda x: re.sub("l", "1", x))
    comb["num_pages_cleaned"] = comb["num_pages_cleaned"].map(add_commata)
    comb["num_pages_cleaned"] = comb["num_pages_cleaned"].map(sub_chars)
    comb["number_pages_total"] = comb["num_pages_cleaned"].map(add_numbers)
    return comb

def save_metadata(data, data_name):
    # function for saving metadata
    with open("{}.tsv".format(data_name), "w", encoding="utf8") as output:
        data.to_csv(output, sep="\t", index=False, line_terminator='\n')   

def main():
    wikibase = get_wikibase_data()
    ## clean wikibase data
    wikibase = clean_pages(wikibase)
    wikibase = wikibase.drop(columns=["distribution_format.value", "bgrf.value", "to_check"])
    save_metadata(wikibase, "cleaned_wikibase")

main()