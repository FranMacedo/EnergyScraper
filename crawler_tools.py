from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as ec
import logging


class CrawlerTools():
    def __init__(self, driver, wait, wait_long):
        self.driver = driver
        self.wait = wait
        self.wait_long = wait_long

    def scroll_to_element(self, element):
        desired_y = (element.size['height'] / 2) + element.location['y']
        window_h = self.driver.execute_script('return window.innerHeight')
        window_y = self.driver.execute_script('return window.pageYOffset')
        current_y = (window_h / 2) + window_y
        scroll_y_by = desired_y - current_y

        self.driver.execute_script("window.scrollBy(0, arguments[0]);", scroll_y_by)
        return

    def try_one_search(self, search):
        self.loading_state()
        try:
            result = self.wait.until(ec.presence_of_element_located(search))
            self.loading_state()
            return result
        except Exception as e:
            logging.error(e)

    def try_all_search(self, search):
        self.loading_state()
        try:
            result = self.wait.until(ec.presence_of_all_elements_located(search))
            self.loading_state()
            return result
        except Exception as e:
            logging.error(e)

    def f_id(self, id):
        return self.try_one_search((By.ID, id))

    def f_xpath(self, x_path, with_all=False):
        if with_all:
            return self.try_all_search((By.XPATH, x_path))
        else:
            return self.try_one_search((By.XPATH, x_path))

    def f_class(self, cl_name):
        return self.try_one_search((By.CLASS_NAME, cl_name))

    def f_css(self, css):
        return self.try_one_search((By.CSS_SELECTOR, css))

    def f_tag(self, tag):
        return self.try_one_search((By.TAG_NAME, tag))

    def f_linktext(self, link):
        return self.try_one_search((By.LINK_TEXT, link))

    def f_linktext_all(self, link):
        return self.try_all_search((By.LINK_TEXT, link))

    def loading_state(self):
        self.wait_long.until(ec.invisibility_of_element_located((By.CSS_SELECTOR, "div[class='backdrop full-screen']")))
        return
