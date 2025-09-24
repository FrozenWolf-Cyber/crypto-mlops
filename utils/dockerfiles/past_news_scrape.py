from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import random
from scrape import scrape_post

def scroll_until_end(page, max_scrolls=50, back_off=1, max_wait=30):
    prev_count = 0
    scrolls = 0
    pause = back_off

    while scrolls < max_scrolls and pause < max_wait:
        # Scroll to bottom
        page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
        scrolls += 1

        # Random small delay to mimic human scrolling
        time.sleep(random.uniform(pause, pause + 1.5))

        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all(attrs={"role": "article"})
        current_count = len(articles)
        print(f"Scroll {scrolls}: found {current_count} articles")

        if current_count == prev_count:
            print("No new articles loaded, increasing pause time.")
            pause *= 2
        else:
            print("New articles loaded, resetting pause time.")
            pause = back_off
            prev_count = current_count
            
        
    html = page.content()
    
    # Print first 2000 characters to terminal (avoid huge dump)
    print("=== PAGE HTML START ===")
    print(html[:2000])
    print("=== PAGE HTML END ===")


    return page

with sync_playwright() as p:
    # Launch browser in headless mode
    browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
    context = browser.new_context(
        viewport={"width": 1280, "height": 800},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/117.0.0.0 Safari/537.36"
    )
    page = context.new_page()
    page.set_default_timeout(60000)
    page.set_default_navigation_timeout(60000)

    coins = ["BTC-USD"]
    all_articles = {}

    for coin in tqdm(coins):
        url = f"https://finance.yahoo.com/quote/{coin}/news/"
        print(f"Scraping URL: {url}")
        page.goto(url, wait_until="domcontentloaded")
        time.sleep(2)  # Allow initial content to load

        # Scroll and load all articles
        page = scroll_until_end(page)

        # Extract links
        html = page.content()
        soup = BeautifulSoup(html, "html.parser")
        articles = soup.find_all(attrs={"role": "article"})
        links = [a.a['href'] for a in articles if a.a]
        print(f"Total articles found for {coin}: {len(links)}")

        all_articles[coin] = scrape_post(links, page)

    browser.close()
