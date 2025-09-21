from playwright.sync_api import sync_playwright
import time
from .scrape import scrape_post
from bs4 import BeautifulSoup
from tqdm import tqdm
def scroll_until_end(page, back_off=1, max_wait=8, max_scrolls=50):
    prev_height = -1
    pause = back_off
    while (pause < max_wait) and max_scrolls:
        # Scroll to bottom
        page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        
        time.sleep(pause)
        new_height = page.evaluate("document.body.scrollHeight")
        if new_height == prev_height:
            print("No new content loaded, increasing pause time.")
            pause*= 2
        else:
            print("New content loaded, resetting pause time.")
            pause = back_off
            max_scrolls -= 1
            
        prev_height = new_height
        
    return page

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.set_default_timeout(60000)              # affects waits/selectors
    page.set_default_navigation_timeout(60000)   # affects navigation

    coins = ["BTCUSDT"]
    all_articles = {}
    for coin in tqdm(coins):
        page.goto(f"https://finance.yahoo.com/quote/{coin}/news/", wait_until="domcontentloaded")
        time.sleep(1)  # Wait for initial content to load
        page = scroll_until_end(page)
        # Page now contains more news—scrape as needed
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all(attrs={"role": "article"})
        links = [a.a['href'] for a in articles]
        print("Number of articles found:", len(links))
        all_articles[coin] = scrape_post(links, page)
    
    browser.close()
    
    
