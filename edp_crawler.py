"""
    Gather energy data from edp distribuição website
"""


# import unittest
# import HtmlTestRunner
import json
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import logging

import pandas as pd
import os
import time


class EdpCrawler():

    def __init__(self, cpe,  date_range, replace=True, headless=True):
        self.replace = replace
        if not date_range:
            print('empty date range... is your date_start before date_end..?')
            raise ValueError
            return

        self.date_range = date_range
        self.final_data = []
        self.base_url = "https://online.edpdistribuicao.pt/pt/Pages/Home.aspx"
        from setup import Setup

        setup_ = Setup()
        setup_.create_downloads_logs_path(robot_type='ENERGY', cpe=cpe)

        if headless:
            connect_result = setup_.connect_headless(self.base_url, cpe)
        else:
            connect_result = setup_.connect(self.base_url, cpe)
        if not connect_result:
            return
        try:
            self.driver, self.wait, self.wait_long, self.inst, self.login_info, self.downloads_path = setup_.driver, setup_.wait, setup_.wait_long, setup_.inst, setup_.login_info, setup_.downloads_path
        except:
            raise Exception
            return

        self.setup = setup_
        if self.inst.abastecimento not in ['BTE', 'MT']:
            print('\n\nNot going for any other than BTE and MT\n\n')
            raise ValueError

        if len(date_range) == 1:
            logging.info(f'\n\n\nTrying {cpe} || {self.date_range[0].strftime("%m-%Y")}\n\n')
        else:
            logging.info(
                f'\n\n\nTrying {cpe} || {self.date_range[0].strftime("%m-%Y")} || {self.date_range[-1].strftime("%m-%Y")}\n\n')

        from crawler_tools import CrawlerTools

        self.ct = CrawlerTools(self.driver, self.wait, self.wait_long)
        # self.tearDown()

    def lista_btn(self):
        self.ct.loading_state()
        try:
            lista = self.driver.find_element_by_xpath("//*[contains(text(),'Lista')]")
            lista.click()
        except NoSuchElementException:
            try:
                lista = self.driver.find_element_by_css_selector('#btn-see-all')
                lista.click()
            except NoSuchElementException:
                pass

    def login(self):
        # change acording to db
        logging.info('login in..')
        tipo_entidade = self.ct.f_linktext('Empresarial')
        tipo_entidade.click()

        user_input = self.ct.f_id('username-enterprise')
        user_input.clear()
        user_input.send_keys(self.login_info.user)

        password_input = self.ct.f_id('password-enterprise')
        password_input.clear()
        password_input.send_keys(self.login_info.password)
        login_buttons = self.ct.f_xpath("//*[contains(text(),'Entrar')]", with_all=True)
        login_btn = [l for l in login_buttons if l.is_enabled()][0]
        login_btn.click()
        self.ct.loading_state()
        logging.info('login successful')

    def search_inst(self, show=False):
        self.driver.get(self.base_url)
        inst = self.inst
        if show:
            logging.info(f'searching cpe {inst.cpe}')
        self.ct.loading_state()
        self.lista_btn()

        search_text_box_container = self.ct.f_class('search-container')
        search_text_box = search_text_box_container.find_element_by_tag_name('input')
        search_text_box.clear()
        search_text_box.send_keys(inst.cpe)

        search_btn = self.ct.f_id('button-addon2')
        search_btn.click()

        self.lista_btn()
        self.ct.loading_state()

    def multiple_tries(self, nr_tries=2):

        for t in range(0, nr_tries):
            logging.info(f'multiple try number {t+1}')
            self.search_inst(True)
            self.loop_rows()
            if not self.date_range:
                break
        return self.date_range

    def loop_rows(self):
        rows = self.ct.f_linktext_all(self.inst.cpe)
        logging.info(f'found {len(rows)} rows')
        dates_success = []
        for row in rows:
            # row = rows[0]
            if not self.date_range:
                logging.info(f'-No more dates missing')
                return
            row_idx = rows.index(row)
            logging.info(f'-Trying row {row_idx + 1}')
            self.driver.get(self.base_url)
            self.search_inst()
            rows_ = self.ct.f_linktext_all(self.inst.cpe)
            row = rows_[row_idx]
            # print('aqui antes do scroll')

            self.ct.scroll_to_element(row)
            row.click()
            self.ct.loading_state()
            # print('aqui antes do consumo tab')
            try:
                consumos_tab = self.ct.f_linktext('Consumos')
            except TimeoutException:
                logging.error('--Cant find consumption tag')
                continue

            self.ct.scroll_to_element(consumos_tab)
            consumos_tab.click()

            self.ct.loading_state()

            dates_available = self.get_years_available()
            if not dates_available:
                logging.error('--No dates requested available in this row')

            for date in dates_available:
                # date = dates_available[8]
                result_month, data_month = self.get_month_data(date)
                if result_month:
                    dates_success.append(date)

            self.date_range = [d for d in self.date_range if d not in dates_success]

    def get_year_options(self):
        years_drop_down = self.ct.f_css('div.select-wrapper')
        year_options = years_drop_down.find_elements_by_tag_name("option")
        return year_options

    def get_years_available(self):
        year_options = self.get_year_options()
        year_options_vals = [x.get_attribute("value") for x in year_options]
        download_years = list(set([str(d.year) for d in self.date_range]))
        download_years_available = [d for d in download_years if d in year_options_vals]
        download_years_not_available = [d for d in download_years if d not in year_options_vals]

        for year in download_years_not_available:
            logging.info(f"--No data for {year} in this row.")

        dates_available = [
            d for d in self.date_range if str(d.year) in download_years_available]
        return dates_available

    def get_month_btn(self, month_nr):
        month_btn = self.ct.f_id(f'btn-month-{month_nr}')
        if month_btn.get_attribute('disabled'):
            logging.info(f'---Month disabled/not available')
            return False, None
        return True, month_btn

    def get_month_data(self, date):
        logging.info(f'--Trying month {date.strftime("%m-%Y")}')

        self.ct.loading_state()
        year_options = self.get_year_options()
        year_select = [y for y in year_options if y.get_attribute('value') == str(date.year)]
        if not year_select:
            logging.info(f'---Cant find year...')
            return False, None
        year_select = year_select[0]
        year_select.click()
        self.ct.loading_state()

        result, month_btn = self.get_month_btn(date.month)
        if not result:
            return False, None
        # if 'btn-primary' not in month_btn.get_attribute('class'):
        month_btn.click()
        self.ct.loading_state()

        max_tries = 10
        nr_try = 0
        result, month_btn = self.get_month_btn(date.month)
        if not result:
            return False, None

        while nr_try <= max_tries:
            if 'btn-primary' in month_btn.get_attribute('class'):
                break
            time.sleep(2)

        if 'btn-primary' not in month_btn.get_attribute('class'):
            logging.error('Cant select button...')
            return False, None

        file_path = self.download_excel()
        if not file_path:
            logging.error(f'---Cant download file')
        else:
            # data_month = self.get_requests_data(date)
            data_month = self.read_excel(file_path, date)

            if not data_month.empty:
                logging.info(f'---Data available')
                result = self.data_to_rede(data_month)
                return result, data_month

        logging.error(f'---Cant get data')
        self.ct.loading_state()
        return False, None

    def download_excel(self, max_tries=10):
        files_before = os.listdir(self.downloads_path)
        self.ct.f_id('btn-export-to-excel').click()
        self.ct.loading_state()

        try_nr = 0
        while try_nr <= max_tries:
            files_after = os.listdir(self.downloads_path)

            if len(files_after) > len(files_before):
                new_file = list(set(files_after) - set(files_before))[0]
                file_path = os.path.join(self.downloads_path, new_file)
                if self.is_excel(file_path):
                    return file_path

            time.sleep(2)
            try_nr += 1

        return None

    def is_excel(self, file_path):
        if 'tmp' in file_path or 'crdownload' in file_path:
            return False
        else:
            return True

    def get_requests_data(self, date):
        """
        Returns:
            pandas DataFrame: list of dicts, where each dict has consumption data (example):
            {'id': 745, 'date': '2020/10/31', 'hour': '24:00', 'active': '5.0',
                'reactiveInductive': '12.0', 'reactiveCapacitive': '0.0'}
        """
        r_all_cons = []
        for r in self.driver.requests:
            if r.response:
                if 'consumption' in r.url.lower():
                    try:
                        r_cons = json.loads(r.response.body)
                        r_body = r_cons['Body']['Result']
                        r_all_cons.append(r_body)
                    except Exception as e:
                        continue
        r_all_cons = [r for r in r_all_cons if r is not None]

        # multiple requests with data. we want the big one of the respective month
        if r_all_cons:
            r_cons = []
            for cons in r_all_cons:

                if len(cons) > 0:
                    if isinstance(cons[0], str):
                        continue
                    if self.compare_month(pd.to_datetime(cons[0]['date'], format='%Y/%m/%d'), date):
                        if not r_cons:
                            r_cons = cons
                        elif len(cons) > len(r_cons):
                            r_cons = cons
            return pd.DataFrame(r_cons)
        return pd.DataFrame()

    def read_excel(self, file_path, date):
        xl_file = pd.read_excel(file_path)
        try:
            date_ = xl_file["Dados Globais"][9]
            split = date_.split("/")
            date_xls = split[0] + split[1]
        except KeyError:
            logging.error(f"File without data - removed")
            os.remove(file_path)
            return pd.DataFrame()

        if date.strftime("%Y%m") != date_xls:
            logging.error(
                f"Dates do not match! \nOriginal: {date.strftime('%Y%m')} ---- InExcel: {date_xls}\nRemove and skip")
            os.remove(file_path)
            return pd.DataFrame()

        df = pd.read_excel(file_path, skiprows=9)

        df.columns = ["date", "hour", "active",
                      "reactiveInductive", "reactiveCapacitive"]
        return df

    def compare_month(self, date_1, date_2):
        date_1_ = date_1.strftime("%Y-%m")
        date_2_ = date_2.strftime("%Y-%m")
        return date_1_ == date_2_

    def data_to_rede(self, data):
        from clean_data import clean_df
        from rede import df_to_rede
        df = clean_df(data)
        date = df.index[0].to_pydatetime()
        return df_to_rede(cpe=self.inst.cpe, date=date, df=df, replace=self.replace)

    def exit(self):
        # to close the browser
        self.driver.close()
        self.setup.delete_downloads_file()
