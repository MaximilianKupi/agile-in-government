import scrapy
import tld
import pathlib
import uuid
import re

from bs4 import BeautifulSoup
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..items import AgilescraperItem
import platform


# Path- and filenames for storing the downloaded HTML Files

if platform.platform() == 'Darwin-18.7.0-x86_64-i386-64bit':
    general_folder_path = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GitHubRepo/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/UK/General/{}'
    general_file_name = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GitHubRepo/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/UK/General/{}/{}.html'
else:
    general_folder_path = '/home/jupyter/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/UK/General/{}'
    general_file_name = '/home/jupyter/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/UK/General/{}/{}.html'






agile_regex = re.compile(r'\bagil.?', re.IGNORECASE)


class AgileScraper(CrawlSpider):
    name = 'General'
    allowed_domains = ['gov.uk']
    start_urls = ['https://www.gov.uk']

    rules = [
        Rule(
            LinkExtractor(
                deny_extensions = ["flv","mov","swf","txt","xml","js","css","zip","gz","rar","7z","tgz","tar","z","gzip","bzip","tar","mp3","mp4","aac","wav","au","wmv","avi","mpg","mpeg","pdf","doc","docx","xls", "xlsm", "csv", "xlsx","ppt","pptx","jpg","jpeg","png","gif","psd","ico","bmp","odt","ods","odp","odb","odg","odf"],
                canonicalize=True,
                unique=True
            ),
            follow=True,
            callback="parse_items"
        )
    ]

    def parse_items(self, response):
        self.logger.info('Item Page: %s', response.url)
        soup = BeautifulSoup(response.body, 'html.parser')
        doctext = soup.get_text()
        if agile_regex.search(doctext):
            item = AgilescraperItem()
            item['id'] = uuid.uuid3(uuid.NAMESPACE_URL, response.url)
            item['url'] = response.url
            item['domain'] = (tld.get_tld(response.url, as_object = True).subdomain + '.' + tld.get_tld(response.url, as_object = True).fld).strip('.')
            item['date1'] = response.selector.xpath('//time/@datetime').get()
            item['date2'] = response.selector.xpath("//html/head/meta[@name='date']/@content").get()
            item['date3'] = response.selector.xpath("substring(substring-after(/html/head/script[@type='application/ld+json']/text(), 'datePublished'), 5, 24)").get()
            item['heading'] = soup.find('h1').get_text(strip = True)
            #item['text'] = doctext
            pathlib.Path(general_folder_path.format(item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 
        


