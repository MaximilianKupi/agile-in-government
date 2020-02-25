# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import csv
import json
import logging

class AgilescraperPipeline(object):
    
    def open_spider(self, spider):
        self.file = open('agile_sites_output.csv', 'a')
        self.csv_writer = csv.writer(self.file)

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if item is not None:
            self.csv_writer.writerow([item['id'], item['url'], item['domain'], item['date1'], item['date2'], item['date3'], item['heading']])
            return item
        else: 
            return None
