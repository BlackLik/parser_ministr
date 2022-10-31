
import sqlite3

import requests
import constant
import pandas
from faker import Faker
import time
from bs4 import BeautifulSoup


class parser_filter:
    def __init__(self, user_agent: str = None) -> None:
        self.fake = Faker()
        self.session = requests.Session()
        self.session.headers = {
            'User-Agent': user_agent if user_agent is not None else str(self.fake.chrome())
        }
        self.session.verify = False

    def get_url_filter(self, code: str) -> str:
        url_filter = "https://profstandart.rosmintrud.ru/obshchiy-informatsionnyy-blok/natsionalnyy-reestr-professionalnykh-standartov/reestr-professionalnykh-standartov/?OKVED_OLD%5B%5D=&inp_OKVED_OLD=&OKVED%5B%5D=&inp_OKVED=&OKZ%5B%5D=&inp_OKZ=&OKZ_010_93%5B%5D=&inp_OKZ_010_93=&OKPDTR%5B%5D=&inp_OKPDTR=&OKSO%5B%5D=&inp_OKSO=&NAME=&KPF=" + \
            code+"&RANGE_PROFACT=&KIND_PROFACT=&DEVELOPERS=&FIO_HEAD=&ADVICE_PQ=&DATE_STATEMENT_FROM=&DATE_STATEMENT_TO=&N_ORDER=&OKSO_2016=&set_filter=%D0%A4%D0%B8%D0%BB%D1%8C%D1%82%D1%80&set_filter=Y#"
        print(url_filter)
        req = self.session.get(url=url_filter)
        soup = BeautifulSoup(req.text, 'html.parser')
        element = soup.select_one('td>ul>li>a')
        return element.get('href')

    def get_orc(self, url: str):
        req = self.session.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        index = 24
        selector_query = 'table:nth-child(5) > tbody:nth-child({}) > tr > td > center'.format(
            index)
        elements = soup.select(selector_query)
        while elements:
            for element in elements:
                # or (len(str(element.text)) < 4 or len(str(element.text)) > 5)
                if "." in str(element.text):
                    print(str(element.text), len(str(element.text)))
                    continue
                # print(element.text, type(element.text))
                print(element.text)
                yield element.text
            check = "table:nth-child(5) > tbody:nth-child({}) > tr:nth-child(2) > td > p".format(
                index+1)
            check_element = soup.select_one(check)
            if check_element is not None:
                break
            index += 1
            selector_query = 'tbody:nth-child({}) > tr > td > center'.format(index)
            elements = soup.select(selector_query)

    def get_code(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        items = soup.select("table tbody tr")
        data = [item.select('td a')[1].text.strip() for item in items]
        return data

    def check_items(self, prof: str, page):
        url = "https://profstandart.rosmintrud.ru/obshchiy-informatsionnyy-blok/natsionalnyy-reestr-professionalnykh-standartov/reestr-professionalnykh-standartov/?OKVED_OLD%5B0%5D=&inp_OKVED_OLD=&OKVED%5B0%5D=&inp_OKVED=&OKZ%5B0%5D=&inp_OKZ=&OKZ_010_93%5B0%5D=&inp_OKZ_010_93=&OKPDTR%5B0%5D=&inp_OKPDTR=&OKSO%5B0%5D=&inp_OKSO=&NAME=&KPF=&RANGE_PROFACT=&KIND_PROFACT=&DEVELOPERS=&FIO_HEAD=&ADVICE_PQ=&DATE_STATEMENT_FROM=&DATE_STATEMENT_TO=&N_ORDER=&OKSO_2016={}&set_filter=Y&PAGEN_1={}&SIZEN_1=20#".format(
            prof, page)
        print(url)
        req = self.session.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        have_items = soup.select("section:nth-child(2) > div > p")

        data = self.get_code(req.text)
        time.sleep(5)
        if len(have_items) == 0:
            # data.append(self.get_code(req.text))
            check_pagination = soup.select(
                "div.bx_pagination_bottom > div.bx_pagination_section_one > div > div > ul li")

            if len(check_pagination) > 0 and page < int(check_pagination[-2].text):
                for i in self.check_items(prof, page+1):
                    data.append(i)

        return data


class App:
    def __init__(self, path_file_input: str = constant.FILE_INPUT) -> None:
        self.con = sqlite3.connect('code.sqlite3')
        self.cur = self.con.cursor()

        self.df = pandas.read_csv(str(path_file_input), )
        self.df["code"] = self.df["code"].astype(str)
        self.df['filter_url'] = None
        self.df['main_url'] = None
        self.df['ORC'] = None
        self.name_table = 'main'
        self.lines = open(path_file_input, 'r').read().split("\n")[1:]
        self.pf = parser_filter(user_agent=constant.USER_AGENT)
        try:
            self.df.to_sql(self.name_table, con=self.con, if_exists='fail')
        except Exception as e:
            print(e)

    def run(self) -> None:
        """Start the app"""
        # pf = parser_filter(user_agent=constant.USER_AGENT)
        for item in self.lines:
            query_selector = """SELECT t.filter_url from main.{} as t where t.code = '{}' """.format(
                self.name_table, item)
            print(query_selector)
            res = self.cur.execute(query_selector)
            fetch_one = res.fetchone()
            fetch_one = fetch_one[0]
            if fetch_one is None:
                try:
                    print(item)
                    url_filter = self.pf.get_url_filter(item)
                    time.sleep(5)
                    query = """UPDATE main
                                SET filter_url = '{}'
                                WHERE code = '{}'""".format(url_filter, item)
                    self.cur.execute(query)
                    # print(res.fetchone())
                    self.con.commit()
                except Exception as e:
                    self.con.rollback()
                    print(e)
        res = self.cur.execute(
            """Select t.code, t.filter_url from main.{} as t
                left join main.orc o on o.code=t.code
                where t.filter_url is not null and o.orc is null""".format(self.name_table))
        for index in res.fetchall()[:1]:

            code, url = index
            url = constant.SITE_URL + url
            print(url)
            try:
                orc_turple = self.pf.get_orc(url)
                for orc in orc_turple:
                    try:
                        query_insert = """INSERT OR IGNORE INTO main.orc (code, orc) VALUES ('{}', {});""".format(
                            code, orc)
                        self.cur.execute(query_insert)
                        self.con.commit()
                    except Exception as e:
                        self.con.rollback()
                        print(e)
            except Exception as e:
                print(e)
            time.sleep(5)
        self.df = pandas.read_sql_query('select * from out', con=self.con)
        self.df.to_excel('out.xlsx', sheet_name='list1')

    def search_data_code_with_prof(self):
        f = open('prof_codes.txt', 'r').read().splitlines()
        for prof_code in f:
            res = self.cur.execute('select count(*) from main.prof p where p.prof = \'{}\';'.format(prof_code))
            # print(res.fetchone()[0])
            if res.fetchone()[0] == 0:
                data = self.pf.check_items(prof_code, 1)
                if len(data) == 0:
                    query_insert = """INSERT or IGNORE INTO main.prof (prof, code) VALUES ('{}', null);""".format(
                        prof_code)
                    try:
                        self.cur.execute(query_insert)
                        self.con.commit()
                    except Exception as e:
                        self.con.rollback()
                        print(e)
                else:
                    for item in data:
                        query_insert = """INSERT or IGNORE INTO main.prof (prof, code) VALUES ('{}', '{}');""".format(
                            prof_code, item)
                        try:
                            self.cur.execute(query_insert)
                            self.con.commit()
                        except Exception as e:
                            self.con.rollback()
                            print(e)
                    # prof_code = '13.02.11'
                    # print(self.pf.check_items(prof_code, 1))


if __name__ == '__main__':
    app = App()
    app.search_data_code_with_prof()
