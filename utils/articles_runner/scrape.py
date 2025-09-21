from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
from tqdm import tqdm
import pandas as pd
import os

def scrape_post(links, page):
    df_path = f"./data/articles/articles.csv"
    if os.path.exists(df_path):
        df = pd.read_csv(df_path)
    else:
        df = pd.DataFrame(columns=["title", "text", "date", "link"])
        
    link_set = set(df["link"])
    articles_scrapped = {}
    for link in tqdm(links):

        if link in link_set:
            print("Already scrapped:", link)
            continue
        
        print("--"*20)
        print("\nProcessing link:", link)
        prefix_allowed = ["https://finance.yahoo.com/"+i for i in ['news', 'personal-finance', 'video', 'm']]
        allow=False
        for prefix in prefix_allowed:
            if link.startswith(prefix):
                allow = True
                break
        if not allow:
            print("!!!Skipping link:", link)
            continue
        
        if "https://finance.yahoo.com/" in link:
            print(link)
            page.goto(link, wait_until="domcontentloaded")
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            date = soup.find("time", class_="byline-attr-meta-time")['datetime']
            title = soup.find("h1", class_="cover-title").text
            
            articles = soup.find(attrs={"data-testid": "article-content-wrapper"})
            if articles is None:
                articles = soup.find_all("div", class_="article-wrap")[-1].div
            else:
                articles = articles.find("div", class_="body-wrap")
                
            if articles is not None:
                p =  articles.find_all("p")
                all_text = []
                for i in p:
                    all_text.append(i.text.strip())
                    # print(i.text.strip())
            else:
                print("Empty article content for link:", link)
                all_text = []
            
            print("Title:", title, len('\n'.join(all_text)), "characters")
            articles_scrapped[link] = {
                "title": title,
                "text": all_text,
                "date": date
            }
            df_ = pd.DataFrame([{
                "title": title,
                "text": all_text,
                "date": date,
                "link": link,
                "price_change": None,
                "label": None    
            }])
            df = pd.concat([df, df_], ignore_index=True)
            df.to_csv(df_path, index=False)
            link_set.add(link)
            
    return articles_scrapped
    
if __name__ == "__main__":
    coins = ["BTCUSDT"]


    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # use False for debugging
        page = browser.new_page()
        all_articles = {}
        for symbol in coins:
            url = f"https://finance.yahoo.com/quote/{symbol}/"

            page.goto(url, wait_until="domcontentloaded")
            html = page.content()
            soup = BeautifulSoup(html, "html.parser")
            articles = soup.find_all(attrs={"role": "article"})

            links = []
            for art in articles:
                print(art.a.get("href"))
                links.append(art.a.get("href"))

            articles_scrapped = scrape_post(symbol, links, page)
            all_articles[symbol] = articles_scrapped
            
        
        browser.close()
        
