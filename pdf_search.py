import psycopg2
import requests
import argparse
import pandas as pd
from io import StringIO, BytesIO
from lxml import etree
from selenium import webdriver
from sqlalchemy import create_engine
import duckduckgo_fun
import bing_fun
import ask_fun
import yandex_fun
import yahoo_fun


def process_google_spreadsheet(url, colnames):
    """
    The function returns the content of a google sheet as pandas DataFrame.
    :param url: The link to a Google sheet published as csv.
    :param colnames: The column name of Google sheet where keywords are.
    :return: Pandas DataFrame.
    """
    r = requests.get(url)
    data = r.content
    df = pd.read_csv(filepath_or_buffer=BytesIO(data),
                     skiprows=1,
                     names=colnames,
                     index_col=False)
    if len(df.columns) == 0:
        df.reset_index(inplace=True)
    return df


SEARCH_ENGINE = 'https://duckduckgo.com/'
"""
    There are five search engines you might use:
    'https://duckduckgo.com/'
    'https://bing.com/'
    'http://www.ask.com/'
    'https://www.yandex.ru/'
    'https://www.yahoo.com/'
    Choose one of them.
"""

google_link = 'NaN'
csv_path = '/home/user/PycharmProjects/Selenium/keywords.txt'
host = 'NaN'
db_name = 'NaN'
db_user = 'NaN'
db_password = 'NaN'

if google_link != 'NaN':
    _google_sheet_link = google_link
    print("We are getting keywords from google sheet!")
    column_name = ["keywords"]
    data_frame = process_google_spreadsheet(url=_google_sheet_link, colnames=column_name)
    print(data_frame)
    keywords = data_frame['keywords'].tolist()
if csv_path != 'NaN':
    _path_to_csv = csv_path
    print("We are getting keywords from csv file!")
    key_df = pd.read_csv(_path_to_csv, sep=',', header=None)
    keywords = key_df[0].tolist()
if host != 'NaN' and db_name != 'NaN' and db_user != 'NaN' and db_password != 'NaN':
    _db_host = host
    _db_name = db_name
    _db_user = db_user
    _db_password = db_password
    print("We are getting keywords from database!")
    _db_port = '5432'
    _engine = create_engine(
        'postgresql://{}:{}@{}:{}/{}'.format(_db_user, _db_password, _db_host, _db_port, _db_name))
    DB = {
        'drivername': 'postgres',
        'database': _db_name,
        'host': _db_host,
        'port': _db_port,
        'username': _db_user,
        'password': _db_password
    }

    dsn = "host={} dbname={} user={} password={}".format(DB['host'],
                                                         DB['database'],
                                                         DB['username'],
                                                         DB['password'])
    connection = psycopg2.connect(dsn)
    cur = connection.cursor()
    connection.autocommit = True
    cur.execute("select keyword from table keywords")
    rows = cur.fetchall()
    keywords = list(rows[0])
if google_link == 'NaN' and csv_path == 'NaN' and host == 'NaN' and db_name == 'NaN' and db_user == 'NaN' and db_password == 'NAN':
    print("No source of keywords have been chosen! Try again!")
    keywords = []

TOPIC = 'test'
FILETYPE = 'pdf'
PAGES_LIMIT = 'NaN'
LINKS_LIMIT = 'NaN'

engine = create_engine(
    'postgresql://textminer:Infrared spectroscopy@ec2-54-202-180-1.us-west-2.compute.amazonaws.com:5432/pdfs')

DB = {
    'drivername': 'postgres',
    'database': 'pdfs',
    'host': 'ec2-54-202-180-1.us-west-2.compute.amazonaws.com',
    'port': '5432',
    'username': 'textminer',
    'password': "'Infrared spectroscopy'"
}

dsn = "host={} dbname={} user={} password={}".format(DB['host'],
                                                     DB['database'],
                                                     DB['username'],
                                                     DB['password'])

connection = psycopg2.connect(dsn)
cur = connection.cursor()
print("We have connected to database")
parser = etree.HTMLParser()
browser = webdriver.PhantomJS()
browser.get(SEARCH_ENGINE)
if SEARCH_ENGINE == 'https://www.yahoo.com/':
    print("We have chosen ", SEARCH_ENGINE, "search engine")
    ALL_LINKS_XPATH = yahoo_fun.ALL_LINKS_XPATH
    LINK_XPATH = yahoo_fun.LINK_XPATH
    SNIPPET_XPATH = yahoo_fun.SNIPPET_XPATH
    TITLE_XPATH = yahoo_fun.TITLE_XPATH
    RESULT_XPATH = yahoo_fun.RESULT_XPATH
    NEXT_PAGE_XPATH = yahoo_fun.NEXT_PAGE_XPATH
    yahoo_fun.yahoo_crawler(SEARCH_ENGINE, FILETYPE, keywords, ALL_LINKS_XPATH, LINK_XPATH, RESULT_XPATH, TITLE_XPATH,
                            SNIPPET_XPATH, NEXT_PAGE_XPATH, TOPIC, browser, parser, engine, PAGES_LIMIT, LINKS_LIMIT)
elif SEARCH_ENGINE == 'https://www.yandex.ru/':
    print("We have chosen ", SEARCH_ENGINE, "search engine")
    ALL_LINKS_XPATH = yandex_fun.ALL_LINKS_XPATH
    LINK_XPATH = yandex_fun.LINK_XPATH
    SNIPPET_XPATH = yandex_fun.SNIPPET_XPATH
    TITLE_XPATH = yandex_fun.TITLE_XPATH
    RESULT_XPATH = yandex_fun.RESULT_XPATH
    NEXT_PAGE_XPATH = yandex_fun.NEXT_PAGE_XPATH
    yandex_fun.yandex_crawler(SEARCH_ENGINE, FILETYPE, keywords, ALL_LINKS_XPATH, LINK_XPATH, RESULT_XPATH, TITLE_XPATH,
                              SNIPPET_XPATH, NEXT_PAGE_XPATH, TOPIC, browser, parser, engine, PAGES_LIMIT, LINKS_LIMIT)
elif SEARCH_ENGINE == 'http://www.ask.com/':
    print("We have chosen ", SEARCH_ENGINE, "search engine")
    ALL_LINKS_XPATH = ask_fun.ALL_LINKS_XPATH
    LINK_XPATH = ask_fun.LINK_XPATH
    SNIPPET_XPATH = ask_fun.SNIPPET_XPATH
    TITLE_XPATH = ask_fun.TITLE_XPATH
    NEXT_PAGE_XPATH = ask_fun.NEXT_PAGE_XPATH
    ask_fun.ask_crawler(SEARCH_ENGINE, FILETYPE, keywords, ALL_LINKS_XPATH, LINK_XPATH, TITLE_XPATH, SNIPPET_XPATH,
                        NEXT_PAGE_XPATH, TOPIC, browser, parser, engine, PAGES_LIMIT, LINKS_LIMIT)
elif SEARCH_ENGINE == 'https://bing.com/':
    print("We have chosen ", SEARCH_ENGINE, "search engine")
    ALL_LINKS_XPATH = bing_fun.ALL_LINKS_XPATH
    LINK_XPATH = bing_fun.LINK_XPATH
    SNIPPET_XPATH = bing_fun.SNIPPET_XPATH
    TITLE_XPATH = bing_fun.TITLE_XPATH
    RESULT_XPATH = bing_fun.RESULT_XPATH
    NEXT_PAGE_XPATH = bing_fun.NEXT_PAGE_XPATH
    bing_fun.bing_crawler(SEARCH_ENGINE, FILETYPE, keywords, ALL_LINKS_XPATH, LINK_XPATH, RESULT_XPATH, TITLE_XPATH,
                          SNIPPET_XPATH, NEXT_PAGE_XPATH, TOPIC, browser, parser, engine, PAGES_LIMIT, LINKS_LIMIT)
elif SEARCH_ENGINE == 'https://duckduckgo.com/':
    print("We have chosen ", SEARCH_ENGINE, "search engine")
    ALL_LINKS_XPATH = duckduckgo_fun.ALL_LINKS_XPATH
    LINK_XPATH = duckduckgo_fun.LINK_XPATH
    SNIPPET_XPATH = duckduckgo_fun.SNIPPET_XPATH
    TITLE_XPATH = duckduckgo_fun.TITLE_XPATH
    duckduckgo_fun.duckduckgo_crawler(SEARCH_ENGINE, FILETYPE, keywords, ALL_LINKS_XPATH, LINK_XPATH, TITLE_XPATH,
                                      SNIPPET_XPATH, TOPIC, browser, parser, engine, PAGES_LIMIT, LINKS_LIMIT)
else:
    print("There is no search engine, you entered.")

cur.close()
connection.close()
