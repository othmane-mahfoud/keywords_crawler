# Importing necessary modules
import tldextract as tld
import urllib.request
import pandas as pd
import validators
import requests
import urllib
import csv
import re

from keywords import keywords_fr, keywords_en, keywords_common
from links import links_to_check, links_to_avoid
from urllib.parse import urlsplit, urlunsplit
from pyspark import SparkContext, SparkConf
from bs4 import BeautifulSoup
from parsel import Selector

# Settings
get_timeout = 15
header = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'
}

# Spark context
sc = SparkContext(appName='Keywords Crawler')

# Read data from .csv file into RDD
input_file_name = 'input.csv'
output_file_name = 'output.csv'

# Choose keywords to use and concatenate them into one list
keywords = keywords_fr + keywords_en + keywords_common


# Get keywords and set header of output file to id, website, title, LinkedIn, and all keywords
def output_header(unique_id='ID'):
    output_file_header = [unique_id, 'Website', 'Title', 'LinkedIn']
    output_file_header.extend(keywords)
    return output_file_header


def initialize_keywords_count():
    keywords_count = {}
    for keyword in keywords:
        keywords_count[keyword] = None
    return keywords_count


def initialize_keywords_list():
    keywords_count_list = []
    for keyword in keywords:
        keywords_count_list.append(None)
    return keywords_count_list


def get_base_url(url):
    base_url = tld.extract(url.lower()).domain
    return base_url


def read_input_file():
    df = pd.read_csv(input_file_name)
    input_dict = df.to_dict(orient='records')
    input_rdd = sc.parallelize(input_dict)
    return input_rdd


def prepare_url(url):
    if not url.startswith('http'):
        url = 'http://' + url
    if validators.url(url):
        return url
    else:
        raise Exception('Invalid URL format: ' + url)


def error_handler(keywords_count_dict):
    keywords_none = []
    for keyword_count in keywords_count_dict.values():
        keywords_none.append(None)
    return None, None, None, keywords_none


def get_title(soup):
    title_tag = soup.select_one('title')
    title = title_tag.get_text()
    return title


def get_language(soup):
    html_tag = soup.select_one('html')
    attributes = html_tag.attrs
    try:
        website_lang = attributes.get('lang')
        if 'fr' in website_lang.lower():
            language = 'fr'
        elif 'en' in website_lang.lower():
            language = 'en'
        else:
            language = 'other'
    except:
        language = None
    return language


def links_initial_cleaning(url, all_links):
    cleaned_links = []
    for link in all_links:
        try:
            link = link.replace('\n', '')
            link = link.replace('   /', '')
        except:
            print('Could replace in link for cleaning')
            link = None
        try:
            if link.endswith('/'):
                link = link[:-1]
            if link.startswith("#"):
                link = None
            if link is not None:
                if link.startswith("/"):
                    split_url = urlsplit(url)
                    link = split_url.scheme + '://' + split_url.netloc + link
                else:
                    link = None
        except:
            print('Could clean link')
            link = None
        cleaned_links.append(link)
    return [link for link in cleaned_links if link]


def get_linkedin(url, all_links):
    split_url = urlsplit(url)
    for link in all_links:
        try:
            split_link = urlsplit(link)
            if get_base_url(split_link.netloc) != get_base_url(split_url.netloc):
                if split_link.path.startswith('/company/'):
                    linkedin = link
                    break
                else:
                    linkedin = None
            else:
                linkedin = None
        except:
            print('Could not split url for LinkedIn')
            linkedin = None
    return linkedin


def process_links(url, cleaned_links):
    links_to_process = [url]
    links_to_process.extend(cleaned_links)
    processed_links = []
    for link in links_to_process:
        processed_link = None
        try:
            url_path = urlsplit(link.lower()).path
            if any(ext in url_path for ext in links_to_avoid):
                processed_link = None
            elif not (any(ext in url_path for ext in links_to_check)):
                processed_link = None
            else:
                processed_link = link
        except:
            print('Could not split url for precessing')
            processed_link = None
        processed_links.append(processed_link)
    return [link for link in processed_links if link]


def clean_soup(response):
    soup = BeautifulSoup(response.text, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text


def keyword_counter(processed_links, keywords_count_dict):
    processed_links = list(set(processed_links))
    for link in processed_links:
        print('Counting keywords on ' + link)
        try:
            link = prepare_url(link)
            response = requests.get(link)
            try:
                text = clean_soup(response)
                try:
                    for keyword in keywords_count_dict.keys():
                        reg_finder = re.compile(keyword, re.IGNORECASE)
                        matches = reg_finder.findall(text)
                        if keywords_count_dict[keyword] is None:
                            keywords_count_dict[keyword] = len(matches)
                        else:
                            keywords_count_dict[keyword] = keywords_count_dict[keyword] + len(matches)
                except:
                    print('Could not count keywords')
                    continue
            except:
                print('Could not get soup for keyword counting')
                continue
        except:
            print('Could not get URL for keyword counting')
            continue
    return keywords_count_dict


def convert_dict_to_list(dict_to_convert):
    new_list = []
    try:
        new_list = [value for value in dict_to_convert.values()]
    except:
        raise Exception('Could not convert dictionary to list')
    return new_list


def crawl_url(row):
    # Get input variable
    unique_id = row['Siren']
    url = row['URL']
    # Init output variables
    title = ''
    language = ''
    linkedin = ''
    keywords_count_dict = initialize_keywords_count()
    keywords_count_list = initialize_keywords_list()
    keywords_after_count_list = keywords_count_list
    try:
        url = prepare_url(url)
        try:
            response = requests.get(url, timeout=get_timeout)
            if response.status_code == requests.codes.ok:
                print("Crawling: " + url)
                try:
                    soup = BeautifulSoup(response.text, 'lxml')
                    # Get website title
                    try:
                        title = get_title(soup)
                    except:
                        print('Could not get title')
                        title = None
                    # Get website language
                    try:
                        language = get_language(soup)
                    except:
                        print('Could not get language')
                        language = None
                    try:
                        # Get all the links in the website
                        selector = Selector(response.text)
                        all_links = selector.xpath('//a/@href').getall()
                        cleaned_links = []
                        processed_links = []
                        # Prepare links
                        try:
                            prepared_links = []
                            for link in all_links:
                                prepared_link = prepare_url(link)
                                prepared_links.append(prepared_link)
                        except:
                            prepared_links = all_links
                        # Get company LinkedIn
                        try:
                            linkedin = get_linkedin(url, prepared_links)
                        except:
                            print('Could not get LinkedIn')
                            linkedin = None
                        # Cleaning and processing links
                        try:
                            cleaned_links = links_initial_cleaning(url, prepared_links)
                        except:
                            print('Could not get clean links')
                            cleaned_links = []
                        try:
                            processed_links = process_links(url, cleaned_links)
                        except:
                            print('Could not get process links')
                            processed_links = []
                        # Counting keywords
                        try:
                            keywords_after_count_dict = keyword_counter(processed_links, keywords_count_dict)
                        except:
                            print('Could not get keywords count')
                            keywords_after_count_dict = keywords_count_dict
                        # Convert keywords dictionary to list
                        try:
                            keywords_after_count_list = convert_dict_to_list(keywords_after_count_dict)
                        except:
                            print('Could not convert dictionary')
                            keywords_after_count_list = keywords_count_list
                    except:
                        print('Could not get Links')
                        linkedin = None
                except:
                    print('Beautiful soup error')
                    title, language, linkedin, keywords_count_list = error_handler(keywords_count_dict)
            else:
                print('Status not 200 OK')
                title, language, linkedin, keywords_count_list = error_handler(keywords_count_dict)
        except:
            print('Could not get URL')
            title, language, linkedin, keywords_count_list = error_handler(keywords_count_dict)
    except:
        print('Invalid URL')
        title, language, linkedin, keywords_count_list = error_handler(keywords_count_dict)
    # building output tuple
    output_tuple = (unique_id, url, title, language, linkedin) + tuple(keywords_after_count_list)
    return output_tuple


input_rdd = read_input_file()
output_rdd = input_rdd.map(lambda row: crawl_url(row))

output_list = output_rdd.collect()
field_names = ['Siren', 'URL', 'title', 'language', 'linkedin']
field_names.extend(keywords)
output_df = pd.DataFrame(output_list, columns=field_names)
output_df.to_csv(output_file_name, index=False)