from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime as dt
import shutil
import logging
import os


class Setup():

    def __init__(self):

        self.cpe = ''
        self.downloads_path = ''
        self.logs_path = ''

    def set_robot_type(self):
        if any([a for a in ['agua', 'water', 'água'] if a in self.robot_type.lower()]):
            self.robot_type = 'WATER'
        elif any([a for a in ['energia', 'energy'] if a in self.robot_type.lower()]):
            self.robot_type = 'ENERGY'
        else:
            self.robot_type = 'OTHER'

    def create_downloads_logs_path(self, robot_type='', cpe=''):
        logger = logging.getLogger()
        for handler in logger.handlers[:]:  # make a copy of the list
            logger.removeHandler(handler)

        self.cpe = cpe
        self.robot_type = robot_type

        self.set_robot_type()
        current_dir = os.getcwd()
        main_downloads_path = os.path.join(current_dir, 'downloads')
        main_logs_path = os.path.join(current_dir, 'logs')
        self.downloads_path = self.create_setup_dir(main_downloads_path, True)
        self.logs_path = self.create_setup_dir(main_logs_path, False)

        logging.basicConfig(
            format='%(asctime)s ::%(levelname)s:: %(message)s',
            # filename=os.path.join(self.logs_path, 'log.log'),
            level=logging.INFO,
            handlers=[
                logging.FileHandler(os.path.join(self.logs_path, 'log.log')),
                logging.StreamHandler()
            ]
        )
        logger = logging.getLogger('seleniumwire')
        logger.setLevel(logging.ERROR)  # Run selenium wire at ERROR level

        if cpe:
            logging.info(f'Starting logging for cpe {cpe}...')
        else:
            logging.info('Starting logging...')

    def create_setup_dir(self, main_dir, empty=False):
        self.create_dir(main_dir)
        main_dir_ = os.path.join(main_dir, self.robot_type)
        self.create_dir(main_dir_, empty)
        main_dir__ = os.path.join(main_dir_, f'CRAWL_{self.cpe}__' + dt.now().strftime('%d_%m_%Y___%Hh%Mmin%Sseg'))
        self.create_dir(main_dir__, True)
        return main_dir__

    def create_dir(self, dir_path, empty=False):
        if not os.path.isdir(dir_path):
            os.mkdir(dir_path)
        else:
            if empty:
                self.delete_files(dir_path)

    def delete_downloads_file(self):
        self.delete_files(self.downloads_path)

    def delete_files(self, dir_path):
        # apaga todos os ficheiros de uma pasta se estiver la alguma coisa
        if dir_path:
            for the_file in os.listdir(dir_path):
                file_path = os.path.join(dir_path, the_file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print(e)

    def connect(self, url, cpe):
        self.cpe = cpe
        self.get_base_info()
        logging.info('Connecting with driver')
        chrome_options = Options()
        chrome_options.add_argument("--whitelisted-ips=127.0.0.1")
        seleniumwire_options = {
            'connection_timeout': None
        }
        prefs = {"download.default_directory": self.downloads_path}
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome("chromedriver/chromedriver.exe",
            options=chrome_options, seleniumwire_options=seleniumwire_options, service_log_path='NUL')

        if not self.driver.command_executor:
            logging.error('ERROR INITIALIZING DRIVER')
            return False
        self.driver.maximize_window()
        self.driver.get(url)

        self.wait = WebDriverWait(self.driver, 30)
        self.wait_long = WebDriverWait(self.driver, 100)
        logging.info('Connection successful')
        return True

    def connect_headless(self, url, cpe):
        self.cpe = cpe
        self.get_base_info()
        logging.info('Connecting with driver')
        # declare variable to store the URL to be visited
        # declare and initialize driver variable
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--whitelisted-ips=127.0.0.1")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36")
        seleniumwire_options = {
            'connection_timeout': None
        }
        prefs = {"download.default_directory": self.downloads_path, "download.prompt_for_download": False, "profile.default_content_settings.popups": 0,
                 "safebrowsing.enabled": False}
        chrome_options.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome("chromedriver/chromedriver.exe",
            options=chrome_options, seleniumwire_options=seleniumwire_options, service_log_path='NUL')
        if not self.driver.command_executor:
            logging.error('ERROR INITIALIZING DRIVER')
            return False
        self.driver.maximize_window()
        self.driver.get(url)
        # self.make_logs_and_downloads_folders()

        self.wait = WebDriverWait(self.driver, 30)
        self.wait_long = WebDriverWait(self.driver, 100)
        logging.info('Connection successful')
        return True

    def get_base_info(self):
        cpe = self.cpe
        from db_connection import GetDB
        db = GetDB(True)
        df_inst = db.instalacaoEnergia()
        try:
            self.inst = df_inst.loc[df_inst.cpe == cpe].iloc[0]
        except Exception as e:
            logging.error(f'Cant find cpe {cpe}: {e}')
            return False
        df_login = db.loginEdp()

        login_info = df_login.loc[df_login.cpe == cpe]
        if login_info.empty:
            login_info = df_login.loc[(df_login.gestao == self.inst.gestao) & (df_login.cpe.isna())]

        try:
            self.login_info = login_info.iloc[0]
        except Exception as e:
            logging.error(f'Cant find login info for {cpe}: {e}')
            return False

        return True
# if __name__ == '__main__':
#   setup = MainSetup('energy', "https://online.edpdistribuicao.pt/pt/Pages/Home.aspx")
