# save as extract_links.py
import requests
from bs4 import BeautifulSoup
import csv

url = "https://www.marketing-mentor.com/pages/trade-list"
resp = requests.get(url, headers={"User-Agent": "link-extractor/1.0"})
resp.raise_for_status()

soup = BeautifulSoup(resp.text, "html.parser")

# change selector if needed; example: links inside the main content or accordion
anchors = soup.select("a")  # or ".accordion__content a" or ".rte a"

links = []
for a in anchors:
    href = a.get("href")
    text = a.get_text(strip=True)
    if href:
        # make absolute urls
        href = requests.compat.urljoin(url, href)
        links.append((text, href))

# print
for t, h in links:
    print(t, h)

# optional: write to CSV
with open("links.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["text", "href"])
    writer.writerows(links)
print(f"Wrote {len(links)} links to links.csv")
