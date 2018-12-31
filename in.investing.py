import csv

try:
    from BeautifulSoup import BeautifulSoup
except ImportError:
    from bs4 import BeautifulSoup

with open('nifty_200.html', 'r') as myfile:
    doc=myfile.read().replace('\n', '')

html = doc
soup = BeautifulSoup(html, "lxml")

data = []
table = soup.find('table', attrs={'id':'cr1'})
table_body = table.find('tbody')

rows = table_body.find_all('tr')
for row in rows:
    cols = row.find_all('td')
    cols = [ele.text.strip() for ele in cols]
    data.append([ele for ele in cols if ele]) # Get rid of empty values

with open("nifty_200.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows(data)
