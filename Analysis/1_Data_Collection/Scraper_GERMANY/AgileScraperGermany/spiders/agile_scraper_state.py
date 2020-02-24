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

agile_regex = re.compile(r'\bagil.?', re.IGNORECASE)


# Path- and filenames for storing the downloaded HTML Files

if platform.platform() == 'Darwin-18.7.0-x86_64-i386-64bit':
    general_folder_path = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GitHubRepo/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/Germany/{}/{}'
    general_file_name = '/Users/mxm/Google Drive/Masterstudium/Inhalte/Master Thesis/GitHubRepo/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/Germany/{}/{}/{}.html'
else:
    general_folder_path = '/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/Germany/{}/{}'
    general_file_name = '/agile-in-government/Analysis/1_Data_Collection/DATA/HTMLs/Germany/{}/{}/{}.html'


# STATE LEVEL ################################


# Baden-Württemberg

class AgileScraperBW(CrawlSpider):
    name = 'Baden-Wuerttemberg'
    allowed_domains = ["stm.baden-wuerttemberg.de", "im.baden-wuerttemberg.de", "fm.baden-wuerttemberg.de", "km-bw.de", "mwk.baden-wuerttemberg.de", "um.baden-wuerttemberg.de", "wm.baden-wuerttemberg.de", "sozialministerium.baden-wuerttemberg.de", "mlr.baden-wuerttemberg.de", "justizministerium-bw.de", "vm.baden-wuerttemberg.de", "baden-wuerttemberg.de"]
    start_urls = ["https://www.stm.baden-wuerttemberg.de", "https://www.im.baden-wuerttemberg.de", "https://www.fm.baden-wuerttemberg.de", "https://www.km-bw.de", "https://www.mwk.baden-wuerttemberg.de", "https://www.um.baden-wuerttemberg.de", "https://www.wm.baden-wuerttemberg.de", "https://www.sozialministerium.baden-wuerttemberg.de", "https://www.mlr.baden-wuerttemberg.de", "https://www.justizministerium-bw.de", "https://www.vm.baden-wuerttemberg.de", "https://www.baden-wuerttemberg.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 
        


#################################
# Bavaria

class AgileScraperBavaria(CrawlSpider):
    name = 'Bavaria'
    allowed_domains = ["bayern.de"]
    start_urls = ["https://www.bayern.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 
        







#######################################
# Hesse


class AgileScraperHesse(CrawlSpider):
    name = 'Hesse'
    allowed_domains = ["hessen.de"]
    start_urls = ["https://www.hessen.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 






#######################################
# Saarland


class AgileScraperSaarland(CrawlSpider):
    name = 'Saarland'
    allowed_domains = ["saarland.de"]
    start_urls = ["https://www.saarland.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 



#######################################
# Rhineland-Palatinate


class AgileScraperRP(CrawlSpider):
    name = 'Rhineland-Palatinate'
    allowed_domains = ["rlp.de"]
    start_urls = ["https://www.rlp.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 




#######################################
# North Rhine-Westphalia 

class AgileScraperNRW(CrawlSpider):
    name = 'North Rhine-Westphalia'
    allowed_domains = ["land.nrw","mkffi.nrw","fm.nrw.de","im.nrw.de","wirtschaft.nrw","mags.nrw","schulministerium.nrw.de","mhkbg.nrw","justiz.nrw","vm.nrw.de","umwelt.nrw.de","mkw.nrw","mbei.nrw"]
    start_urls = ["https://www.land.nrw/","https://www.mkffi.nrw/","http://www.fm.nrw.de/","http://www.im.nrw.de/","https://www.wirtschaft.nrw/","https://www.mags.nrw/","https://www.schulministerium.nrw.de/","https://www.mhkbg.nrw/","https://www.justiz.nrw/","http://www.vm.nrw.de/","https://www.umwelt.nrw.de/","https://www.mkw.nrw/","http://www.mbei.nrw/"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 



#######################################
# Brandenburg

class AgileScraperBrandenburg(CrawlSpider):
    name = 'Brandenburg'
    allowed_domains = ["brandenburg.de"]
    start_urls = ["https://www.brandenburg.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 



#######################################
# Berlin

class AgileScraperBerlin(CrawlSpider):
    name = 'Berlin'
    allowed_domains = ["berlin.de"]
    start_urls = ["https://www.berlin.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 



#######################################
# Mecklenburg-Vorpommern

class AgileScraperMV(CrawlSpider):
    name = 'Mecklenburg-Vorpommern'
    allowed_domains = ["regierung-mv.de"]
    start_urls = ["https://www.regierung-mv.de/Landesregierung/stk/","https://www.regierung-mv.de/Landesregierung/im/","https://www.regierung-mv.de/Landesregierung/jm/","https://www.regierung-mv.de/Landesregierung/fm/","https://www.regierung-mv.de/Landesregierung/wm/","https://www.regierung-mv.de/Landesregierung/lm/","https://www.regierung-mv.de/Landesregierung/bm/","https://www.regierung-mv.de/Landesregierung/em/","https://www.regierung-mv.de/Landesregierung/sm/"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 


#######################################
# Saxony

class AgileScraperSaxony(CrawlSpider):
    name = 'Saxony'
    allowed_domains = ["sachsen.de"]
    start_urls = ["https://www.sachsen.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 




#######################################
# Saxony-Anhalt

class AgileScraperSA(CrawlSpider):
    name = 'Saxony-Anhalt'
    allowed_domains = ["sachsen-anhalt.de"]
    start_urls = ["https://www.sachsen-anhalt.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 






#######################################
# Thuringia

class AgileScraperThuringia(CrawlSpider):
    name = 'Thuringia'
    allowed_domains = ["staatskanzlei-thueringen.de", "bildung.thueringen.de", "innen.thueringen.de", "justiz.thueringen.de", "finanzen.thueringen.de", "wirtschaft.thueringen.de", "tmasgff.de", "umwelt.thueringen.de", "infrastruktur-landwirtschaft.thueringen.de", "landesregierung-thueringen.de", "thueringen.de"]
    start_urls = ["https://www.staatskanzlei-thueringen.de", "https://bildung.thueringen.de", "https://innen.thueringen.de", "https://justiz.thueringen.de", "https://finanzen.thueringen.de", "https://wirtschaft.thueringen.de", "http://www.tmasgff.de", "https://umwelt.thueringen.de", "https://infrastruktur-landwirtschaft.thueringen.de", "https://www.landesregierung-thueringen.de"]

    rules = [
        Rule(
            LinkExtractor(
                deny_extensions = ["flv","mov","swf","txt","xml","js","css","zip","gz","rar","7z","tgz","tar","z","gzip","bzip","tar","mp3","mp4","aac","wav","au","wmv","avi","mpg","mpeg","pdf","doc","docx","xls", "xlsm", "csv", "xlsx","ppt","pptx","jpg","jpeg","png","gif","psd","ico","bmp","odt","ods","odp","odb","odg","odf"],
                deny=("statistik.thueringen.de",),
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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 




#######################################
# Lower Saxony

class AgileScraperLS(CrawlSpider):
    name = 'Lower Saxony'
    allowed_domains = ["niedersachsen.de"]
    start_urls = ["https://www.niedersachsen.de"]

    rules = [
        Rule(
            LinkExtractor(
                deny_extensions = ["flv","mov","swf","txt","xml","js","css","zip","gz","rar","7z","tgz","tar","z","gzip","bzip","tar","mp3","mp4","aac","wav","au","wmv","avi","mpg","mpeg","pdf","doc","docx","xls", "xlsm", "csv", "xlsx","ppt","pptx","jpg","jpeg","png","gif","psd","ico","bmp","odt","ods","odp","odb","odg","odf"],
                deny=("/assets/image/", ),
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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 





#######################################
# Hamburg


class AgileScraperHamburg(CrawlSpider):
    name = 'Hamburg'
    allowed_domains = ["hamburg.de"]
    start_urls = ["https://www.hamburg.de/basfi","https://www.hamburg.de/bgv","https://www.hamburg.de/justizbehoerde","https://www.hamburg.de/bue","https://www.hamburg.de/bkm","https://www.hamburg.de/fb","https://www.hamburg.de/bwvi","https://www.hamburg.de/bwfg","https://www.hamburg.de/innenbehoerde","https://www.hamburg.de/bsw","https://www.hamburg.de/bsb","https://www.hamburg.de/senatskanzlei","https://www.hamburg.de/personalamt"]

    rules = [
        Rule(
            LinkExtractor(
                deny_extensions = ["flv","mov","swf","txt","xml","js","css","zip","gz","rar","7z","tgz","tar","z","gzip","bzip","tar","mp3","mp4","aac","wav","au","wmv","avi","mpg","mpeg","pdf","doc","docx","xls", "xlsm", "csv", "xlsx","ppt","pptx","jpg","jpeg","png","gif","psd","ico","bmp","odt","ods","odp","odb","odg","odf"],
                deny=("immobilien.hamburg.de", "https://www.hamburg.de/branchenbuch/", "https://www.hamburg.de/tickets/", ), 
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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 



#######################################
# Bremen

class AgileScraperBremen(CrawlSpider):
    name = 'Bremen'
    allowed_domains = ["bremen.de"]
    start_urls = ["https://www.bremen.de"]

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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 





#######################################
# Schleswig-Holstein 


class AgileScraperSH(CrawlSpider):
    name = 'Schleswig-Holstein'
    allowed_domains = ["schleswig-holstein.de"]
    start_urls = ["https://www.schleswig-holstein.de/DE/Landesregierung/I/i_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/VI/vi_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/VIII/viii_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/II/ii_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/III/iii_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/IV/iv_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/V/v_node.html","https://www.schleswig-holstein.de/DE/Landesregierung/VII/vii_node.html"]


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
            pathlib.Path(general_folder_path.format(self.name, item['domain'])).mkdir(parents=True, exist_ok=True)
            filename = general_file_name.format(self.name, item['domain'], item['id'])
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log('Saved file %s' % filename)
            return item
        else:
            self.log('skipping')
            return 
