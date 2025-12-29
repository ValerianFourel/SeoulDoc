"""
Frame switching utilities for Naver Maps
Switches between searchIframe (left) and entryIframe (right)
"""


def switch_left(driver):
    """
    Switch to search results frame (left side)
    This is the searchIframe
    """
    driver.switch_to.default_content()
    driver.switch_to.frame('searchIframe')


def switch_right(driver):
    """
    Switch to detail/entry frame (right side)
    This is the entryIframe
    """
    driver.switch_to.default_content()
    driver.switch_to.frame('entryIframe')