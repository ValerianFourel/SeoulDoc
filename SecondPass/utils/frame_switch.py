from selenium.webdriver.common.by import By

def switch_left(driver):
    driver.switch_to.parent_frame()
    left_iframe = driver.find_element(By.XPATH,'//*[@id="searchIframe"]')
    driver.switch_to.frame(left_iframe)
    
def switch_right(driver):
    driver.switch_to.parent_frame()
    right_iframe = driver.find_element(By.XPATH,'//*[@id="entryIframe"]')
    driver.switch_to.frame(right_iframe)