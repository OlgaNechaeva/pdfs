import requests
import pandas as pd
from io import StringIO, BytesIO
from lxml import etree
from time import sleep
from random import randint
from selenium.webdriver.common.keys import Keys


def process_google_spreadsheet(url, colnames):
    """
    Simple wrapper around pandas and requests to retrieve google spreadsheet as pandas DataFrame.
    :param url: Spreadsheet's url (must be published from google docs web-interface to csv format first.
    :param colnames: Column names of spreadsheet
    :return: Pandas DataFrame
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


def duckduckgo_crawler(_search_engine, _file_type, list_of_keywords, _all_links_xpath, _link_xpath,
                       _title_xpath, _snippet_xpath, _topic, _browser, _parser, _engine, _pages_limit, _links_limit):
    for a_keyword in list_of_keywords:
        elem = _browser.find_element_by_name("q")
        elem.clear()
        sleep(randint(3, 5))
        elem.send_keys("{} filetype {}".format(a_keyword, _file_type))  # in search box of the site
        elem.send_keys(Keys.RETURN)
        sleep(randint(3, 5))
        assert "No results found." not in _browser.page_source
        dict_links = []
        all_links = []
        len_of_page = _browser.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
        match = False
        while(match == False):
            lastCount = len_of_page
            sleep(5)
            len_of_page = _browser.execute_script(
                "window.scrollTo(0, document.body.scrollHeight);var lenOfPage=document.body.scrollHeight;return lenOfPage;")
            if lastCount == len_of_page:
                match = True
        page_xml = _browser.page_source
        tree = etree.parse(StringIO(page_xml), _parser)
        links = tree.xpath(_all_links_xpath)
        number_of_page = 0
        _links_count = 0
        page = 1
        for a_link in range(0, len(_browser.find_elements_by_xpath(
                ".//div[@class='result results_links_deep highlight_d']//h2/a[@href][1]"))):
            number_of_page += 1
            if number_of_page % 10 == 0:
                page += 1
            print(a_link, "  ", ''.join(links[a_link].xpath(_link_xpath)))
            if page == _pages_limit:
                break
            else:
                if '.{}'.format(_file_type) in ''.join(links[a_link].xpath(_link_xpath)):
                    _links_count += 1
                    if _links_count == _links_limit:
                        break
                    else:
                        if ''.join(links[a_link].xpath(_link_xpath)) not in all_links:
                            print(a_link)
                            print(''.join(links[a_link].xpath(_link_xpath)),
                                  '\n',
                                  ''.join(links[a_link].xpath(_title_xpath)),
                                  '\n',
                                  ''.join(links[a_link].xpath(_snippet_xpath)))
                            all_links.append(''.join(links[a_link].xpath(_link_xpath)))
                            dict_links.append({'link': ''.join(links[a_link].xpath(_link_xpath)),
                                               'keyword': a_keyword,
                                               'page_number': page,
                                               'result_stats': '',
                                               'title': ''.join(links[a_link].xpath(_title_xpath)),
                                               'author_and_date': '',
                                               'snippet': ' '.join(links[a_link].xpath(_snippet_xpath)),
                                               'related_articles': '',
                                               'cited_by': '',
                                               'search_engine': _search_engine,
                                               'topic': _topic,
                                               'file_type': _file_type})
        sql_table = pd.DataFrame(dict_links)
        sql_table.to_sql('search_results', _engine, if_exists='append', index=False)
        sleep(randint(20, 30))


ALL_LINKS_XPATH = ".//div[@class='result results_links_deep highlight_d']"
LINK_XPATH = ".//h2/a[1]/@href"
SNIPPET_XPATH = ".//div[@class='result__snippet']/*/text()|.//div[@class='result__snippet']/text()"
TITLE_XPATH = ".//h2/a[1]/text()"
