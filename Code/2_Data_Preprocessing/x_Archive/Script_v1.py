# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import csv
from bs4 import BeautifulSoup
import re
import os
import pandas as pd
from lxml import etree
try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
import dateutil
from dateutil.parser import *
from datetime import datetime


# assigning the regex search terms
agile_regex = re.compile(r'\bagile\b|\bagility\b', re.IGNORECASE | re.UNICODE)
agile_method_regex = re.compile(r'\bagile\b[^.]*?\bmethod\w*?\b', re.IGNORECASE | re.UNICODE)

agile_context_regex = re.compile(r'\s*([^\s]+?)\s+([^\s]+?)\s+([^\s]+?)\s+([^\s]+?)\s+agil.*?\s+([^\s]+)\s+([^\s]+?)\s+([^\s]+?)\s+([^\s]+?)\s+', re.IGNORECASE | re.UNICODE)


def process_state(state):
    csv_main = csv.DictReader(open("/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/ALL_CRAWLS/UK/CSV_State/agile_sites_output_Germany_{}.csv".format(state)), fieldnames=["id", 'url', 'domain', 'date1', 'date2', 'date3', 'heading'])
    array = {}
    for line in csv_main: 
        array[line["id"]] = line
    text_dir = "/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/PYTHON/Analysis/DATA/TextFiles/Germany/{}".format(state)
    pathlib.Path(text_dir).mkdir(parents=True, exist_ok=True)

    for id, line in array.items():
        print("processing {}".format(id))
        id = line["id"]
        domain = line["domain"]
        path = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/ALL_CRAWLS/UK/State/{}/{}/{}.html'.format(state, domain, id)
        try:  
            soup = BeautifulSoup(open(path), "html.parser")
        except UnicodeDecodeError:
            soup = BeautifulSoup(open(path, encoding='windows-1252'), "html.parser")    
        doctext = soup.get_text().replace("\n", " ")
        with open("{}/{}.txt".format(text_dir, id), "w") as textfile:
            textfile.write(doctext)
        
        date4_element = soup.select_one("span.date")
        date4 = ""
        if date4_element is not None:
            date4 = date4_element.get_text()
        
        #Getting date5

        htmlparser = etree.HTMLParser()    
        try:
            tree = etree.parse(open(path), htmlparser)
        except UnicodeDecodeError:
            tree = etree.parse(open(path, encoding='windows-1252'), htmlparser)
        date5 = tree.xpath("substring(substring-after(/html//script[@type='application/ld+json']/text(), 'datePublished'), 4, 23)")

        # a special cases for date 5
        date5_1 = tree.xpath("substring(substring-after(/html//script[@type='application/ld+json']/text(), 'datePublished'), 4, 25)")


        # Getting date6

        date6_element = soup.select_one("h1#page-title + p")
        date6 = ""
        if date6_element is not None:
            date6 = date6_element.get_text()


        # Getting date7 from National Archives --> the date the site was archived on --> the date the site was released on would have been even earlier

        date7 = tree.xpath("substring(substring-after(/html/head/script/text(), 'timestamp'),9,14)")


        # assinging the already crawled dates
        date1 = line["date1"]
        date2 = line["date2"]


        # Storing the oldest date as final date variable in python date format
        date_vars = [date1, date2, date4, date5, date5_1, date6, date7]
        
        final_date = None 

        for date_var in date_vars:
            try:
                if final_date is None:
                    final_date = parse(date_var, ignoretz = True)
                elif parse(date_var, ignoretz=True) < final_date:
                    final_date = parse(date_var, ignoretz=True)
            except dateutil.parser._parser.ParserError:
                pass 


        #finding the matches for agile and agility
        agile_term = []
        agile_term = agile_regex.findall(doctext)

        #finding the matches for agile...methods
        agile_method = []
        agile_method = agile_method_regex.findall(doctext)

        # finding the context for the agile
        agile_context = []
        agile_context = agile_context_regex.search(doctext)

        agile_context_pre = ""
        agile_context_post = ""
        
        if agile_context is not None:
            agile_context_pre = " ".join(agile_context.group(1,2,3,4))
            agile_context_post = " ".join(agile_context.group(5,6,7,8))
        

        # assigning all the variables to the items

        if len(agile_term) == 0:
            line["agile_term"] = "" 
        else:
            line["agile_term"] = ",".join(agile_term)
        
        if len(agile_method) == 0:
            line["agile_method"] = "" 
        else:
            line["agile_method"] = ",".join(agile_method)

        line["agile_context_pre"] = agile_context_pre
        line["agile_context_post"] = agile_context_post 
        line["date4"] = date4
        line["date5"] = date5
        line["date5_1"] = date5_1
        line["date6"] = date6
        line["date7"] = date7
        line["final_date"] = final_date
        line["country"] = "Britain"
        line["level"] = "general"
        line["text_file_loc"] = "{}/{}.txt".format(text_dir, id)
    
    outputfile = open("/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/PYTHON/Analysis/DATA/Germany/{}.csv".format(state), "w")
    writer = csv.DictWriter(outputfile, fieldnames=["id", "country", "level", 'url', 'domain', 'date1', 'date2', 'date3', 'date4', 'date5', 'date5_1', 'date6', 'date7', 'final_date', 'heading', 'agile_term', 'agile_method', 'agile_context_pre', 'agile_context_post', 'text_file_loc'])
    writer.writeheader()
    for id, line in array.items():
        writer.writerow(line)
    outputfile.close()




def cleaning_state(state):
    inputFileName = "/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/PYTHON/Analysis/DATA/Germany/{}.csv".format(state)
    outputFileName = "/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GithubRepository/Analysis/PYTHON/Analysis/DATA/Germany/{}_v1.csv".format(state)

    with open(inputFileName, newline='') as inFile, open(outputFileName, 'w', newline='') as outFile:
        df = pd.read_csv(inFile)
        df.sort_values(by=['final_date'])
        df.dropna(axis=0, how='any', thresh=None, subset=(['agile_term', 'agile_context_pre', 'agile_context_post']), inplace=True)
        df.drop_duplicates(subset=(['heading', 'agile_context_pre', 'agile_context_post']), inplace=True)
        df['heading'] = df['heading'].astype(str)
        #deleting false positives
        df = df[~df.agile_context_post.str.startswith("(")] # dropdown menu entry
        df = df[~df.agile_context_pre.str.endswith(")")] # dropdown menu entry
        df = df[~df.agile_context_pre.str.endswith("EB")] # supplier list
        df = df[~df.agile_context_post.str.match('and battle-winning armed forces')] #army related agility
        df = df[~df.agile_context_pre.str.match('Atom CategoriesCategories Select Category')] # dopdown menu entry
        df = df[~df.agile_context_pre.str.match('RAF to Force Generate')] # Entry on Air Commanders
        df = df[~df.agile_context_post.str.startswith("Trains")] # name of a company "Agility Trains"
        df = df[~df.agile_context_post.str.startswith("trains")] # name of a company "Agility Trains"
        df.to_csv(outFile)



