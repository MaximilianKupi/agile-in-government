# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import json
import logging
import platform

# Path- and filenames for storing the downloaded HTML Files

if platform.platform() == 'Darwin-18.7.0-x86_64-i386-64bit':
    general_file_name = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GitHubRepo/agile-in-government/Analysis/1_Data_Collection/DATA/CSVs/Germany/agile_sites_output_Germany_{}.csv'
else:
    general_file_name = '/home/jupyter/agile-in-government/Analysis/1_Data_Collection/DATA/CSVs/Germany/agile_sites_output_Germany_{}.csv'



# Pipeline
class AgileScraperGermanyPipeline(object):
    
    def open_spider(self, spider):
        self.file = open(general_file_name.format(spider.name), 'a')
        self.csv_writer = csv.writer(self.file)

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if item is not None:
            self.csv_writer.writerow([item['id'], item['url'], item['domain'], item['date1'], item['date2'], item['date3'], item['heading']])
            return item
        else: 
            return None
