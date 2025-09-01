from playwright.sync_api import sync_playwright
import pickle
from bs4 import BeautifulSoup
from tqdm import tqdm

symbol = "BTC-USD"
url = f"https://finance.yahoo.com/quote/{symbol}/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # use False for debugging
    page = browser.new_page()
    link = "https://finance.yahoo.com/personal-finance/credit-cards/article/southwest-airlines-assigned-seating-184222508.html"
    print(link)
    page.goto(link, wait_until="domcontentloaded")
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    articles = soup.find(attrs={"data-testid": "article-content-wrapper"})
    
    # title = articles.find("h1", class_="cover-title").text
    # print("Title:", title)
    
    # p =  articles.find("div", class_="body-wrap").find_all("p")
    # all_text = []
    # for i in p:
    #     all_text.append(i.text.strip())
    #     print(i.text.strip())
        
    pickle.dump(html, open("temp.pkl", "wb"))
    browser.close()
