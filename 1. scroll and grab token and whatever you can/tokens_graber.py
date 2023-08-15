﻿
import urllib.request , urllib.parse , urllib.error
import requests
import sqlite3
from bs4 import BeautifulSoup
import json


conn = sqlite3.connect('divar.sqlite')
cur = conn.cursor()
cur.execute('DROP TABLE IF EXISTS tokens_T')
cur.execute('CREATE TABLE tokens_T(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE , token_C TEXT , title_C TEXT , status_C TEXT , price_C TEXT , store_C TEXT , description_C TEXT)')

main_url = 'https://divar.ir/s/iran/refrigerator-freezer'

html = urllib.request.urlopen(main_url).read()

soup = BeautifulSoup(html , 'html.parser')
tags = soup('article')
count = 0
for tag in tags :

    token = tag.get('token' , None)
    
    title = tag.h2.get_text(strip=True)
    status = tag.find('div', class_='kt-post-card__description').get_text(strip=True)
    price = tag.find('div', class_='kt-post-card__description').find_next_sibling().get_text(strip=True)


    store_look = tag.find('span', class_='kt-post-card__red-text')
    store = store_look.get_text(strip=True) if store_look else 0 

    print("token:", token)
    print("title:", title)
    print("status:", status)
    print("price:", price)
    print("store:", store)


    #cur.execute('INSERT INTO tokens_T(token_C) VALUES (?)' , (tag.get('token' , None) , ))

    cur.execute('INSERT INTO tokens_T(token_C, title_C, status_C, price_C, store_C) VALUES (?, ?, ?, ?, ?)',
            (token, title, status, price, store))
    conn.commit()
    
    count += 1
    if count == 24 : break

last_post_date = int(input('Enter last_post_date value :'))
##https://s100.divarcdn.com/statics/2023/08/bootstrap-5672.91eb43ac.desktop.js
#t.lastPostDate

scroll_api_url = 'https://api.divar.ir/v8/web-search/1/refrigerator-freezer'
#headers = {'Content-Type': 'application/json'}

payload ={
    "page": 1,
    "json_schema": {
        "cities": [
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
            "32",
            "33",
            "34",
            "35",
            "36",
            "37",
            "38",
            "39",
            "314",
            "316",
            "317",
            "318",
            "602",
            "660",
            "662",
            "663",
            "664",
            "665",
            "671",
            "706",
            "707",
            "708",
            "709",
            "710",
            "743",
            "744",
            "745",
            "746",
            "747",
            "748",
            "749",
            "750",
            "751",
            "752",
            "753",
            "754",
            "756",
            "759",
            "760",
            "761",
            "762",
            "763",
            "764",
            "765",
            "766",
            "767",
            "768",
            "769",
            "770",
            "771",
            "772",
            "773",
            "774",
            "775",
            "776",
            "777",
            "778",
            "779",
            "780",
            "781",
            "782",
            "783",
            "784",
            "785",
            "786",
            "787",
            "788",
            "789",
            "790",
            "791",
            "792",
            "793",
            "794",
            "795",
            "796",
            "797",
            "798",
            "799",
            "800",
            "802",
            "803",
            "804",
            "805",
            "806",
            "807",
            "808",
            "809",
            "810",
            "811",
            "812",
            "813",
            "814",
            "815",
            "816",
            "817",
            "818",
            "819",
            "820",
            "821",
            "822",
            "823",
            "824",
            "825",
            "826",
            "827",
            "828",
            "829",
            "830",
            "831",
            "832",
            "833",
            "834",
            "835",
            "836",
            "837",
            "838",
            "839",
            "840",
            "841",
            "842",
            "843",
            "844",
            "845",
            "846",
            "847",
            "848",
            "849",
            "850",
            "851",
            "852",
            "853",
            "854",
            "855",
            "856",
            "857",
            "858",
            "859",
            "860",
            "861",
            "862",
            "863",
            "864",
            "865",
            "866",
            "867",
            "868",
            "869",
            "870",
            "871",
            "872",
            "873",
            "874",
            "1683",
            "1684",
            "1686",
            "1687",
            "1688",
            "1689",
            "1690",
            "1691",
            "1692",
            "1693",
            "1694",
            "1695",
            "1696",
            "1697",
            "1698",
            "1699",
            "1700",
            "1701",
            "1702",
            "1703",
            "1706",
            "1707",
            "1708",
            "1709",
            "1710",
            "1711",
            "1712",
            "1713",
            "1714",
            "1715",
            "1716",
            "1717",
            "1718",
            "1719",
            "1720",
            "1721",
            "1722",
            "1723",
            "1724",
            "1725",
            "1726",
            "1727",
            "1728",
            "1729",
            "1730",
            "1731",
            "1732",
            "1733",
            "1734",
            "1735",
            "1736",
            "1737",
            "1738",
            "1739",
            "1740",
            "1741",
            "1742",
            "1743",
            "1744",
            "1745",
            "1746",
            "1747",
            "1748",
            "1749",
            "1750",
            "1751",
            "1752",
            "1753",
            "1754",
            "1755",
            "1756",
            "1757",
            "1758",
            "1759",
            "1760",
            "1761",
            "1762",
            "1763",
            "1764",
            "1765",
            "1766",
            "1767",
            "1768",
            "1769",
            "1770",
            "1771",
            "1772",
            "1773",
            "1774",
            "1775",
            "1776",
            "1777",
            "1778",
            "1779",
            "1780",
            "1781",
            "1782",
            "1783",
            "1784",
            "1785",
            "1786",
            "1787",
            "1788",
            "1789",
            "1790",
            "1791",
            "1792",
            "1793",
            "1794",
            "1795",
            "1796",
            "1797",
            "1798",
            "1799",
            "1800",
            "1801",
            "1802",
            "1803",
            "1804",
            "1805",
            "1806",
            "1807",
            "1808",
            "1809",
            "1810",
            "1811",
            "1812",
            "1813",
            "1814",
            "1815",
            "1816",
            "1817",
            "1818",
            "1819",
            "1820",
            "1821",
            "1822",
            "1823",
            "1824",
            "1825",
            "1826",
            "1827",
            "1828",
            "1829",
            "1830",
            "1831",
            "1832",
            "1833",
            "1834",
            "1835",
            "1836",
            "1837",
            "1839",
            "1840",
            "1841",
            "1842",
            "1843",
            "1844",
            "1845",
            "1846",
            "1847",
            "1848",
            "1849",
            "1850",
            "1851",
            "1852",
            "1853",
            "1854",
            "1855",
            "1856",
            "1858",
            "1859",
            "1860",
            "1861",
            "1862",
            "1863",
            "1864",
            "1865",
            "1866",
            "1867",
            "1868",
            "1869",
            "1870",
            "1871",
            "1872",
            "1873",
            "1874",
            "1875",
            "1876"
        ],
        "category": {
            "value": "refrigerator-freezer"
        },
        "goods-business-type": {
            "value": "all"
        }
    },
    "last-post-date": last_post_date
}

response = requests.post(scroll_api_url, json=payload)#, headers=headers)
data = response.json()
last_post_date = data['last_post_date']

while True:
    payload ={
    "page": 1,
    "json_schema": {
        "cities": [
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "10",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "25",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
            "32",
            "33",
            "34",
            "35",
            "36",
            "37",
            "38",
            "39",
            "314",
            "316",
            "317",
            "318",
            "602",
            "660",
            "662",
            "663",
            "664",
            "665",
            "671",
            "706",
            "707",
            "708",
            "709",
            "710",
            "743",
            "744",
            "745",
            "746",
            "747",
            "748",
            "749",
            "750",
            "751",
            "752",
            "753",
            "754",
            "756",
            "759",
            "760",
            "761",
            "762",
            "763",
            "764",
            "765",
            "766",
            "767",
            "768",
            "769",
            "770",
            "771",
            "772",
            "773",
            "774",
            "775",
            "776",
            "777",
            "778",
            "779",
            "780",
            "781",
            "782",
            "783",
            "784",
            "785",
            "786",
            "787",
            "788",
            "789",
            "790",
            "791",
            "792",
            "793",
            "794",
            "795",
            "796",
            "797",
            "798",
            "799",
            "800",
            "802",
            "803",
            "804",
            "805",
            "806",
            "807",
            "808",
            "809",
            "810",
            "811",
            "812",
            "813",
            "814",
            "815",
            "816",
            "817",
            "818",
            "819",
            "820",
            "821",
            "822",
            "823",
            "824",
            "825",
            "826",
            "827",
            "828",
            "829",
            "830",
            "831",
            "832",
            "833",
            "834",
            "835",
            "836",
            "837",
            "838",
            "839",
            "840",
            "841",
            "842",
            "843",
            "844",
            "845",
            "846",
            "847",
            "848",
            "849",
            "850",
            "851",
            "852",
            "853",
            "854",
            "855",
            "856",
            "857",
            "858",
            "859",
            "860",
            "861",
            "862",
            "863",
            "864",
            "865",
            "866",
            "867",
            "868",
            "869",
            "870",
            "871",
            "872",
            "873",
            "874",
            "1683",
            "1684",
            "1686",
            "1687",
            "1688",
            "1689",
            "1690",
            "1691",
            "1692",
            "1693",
            "1694",
            "1695",
            "1696",
            "1697",
            "1698",
            "1699",
            "1700",
            "1701",
            "1702",
            "1703",
            "1706",
            "1707",
            "1708",
            "1709",
            "1710",
            "1711",
            "1712",
            "1713",
            "1714",
            "1715",
            "1716",
            "1717",
            "1718",
            "1719",
            "1720",
            "1721",
            "1722",
            "1723",
            "1724",
            "1725",
            "1726",
            "1727",
            "1728",
            "1729",
            "1730",
            "1731",
            "1732",
            "1733",
            "1734",
            "1735",
            "1736",
            "1737",
            "1738",
            "1739",
            "1740",
            "1741",
            "1742",
            "1743",
            "1744",
            "1745",
            "1746",
            "1747",
            "1748",
            "1749",
            "1750",
            "1751",
            "1752",
            "1753",
            "1754",
            "1755",
            "1756",
            "1757",
            "1758",
            "1759",
            "1760",
            "1761",
            "1762",
            "1763",
            "1764",
            "1765",
            "1766",
            "1767",
            "1768",
            "1769",
            "1770",
            "1771",
            "1772",
            "1773",
            "1774",
            "1775",
            "1776",
            "1777",
            "1778",
            "1779",
            "1780",
            "1781",
            "1782",
            "1783",
            "1784",
            "1785",
            "1786",
            "1787",
            "1788",
            "1789",
            "1790",
            "1791",
            "1792",
            "1793",
            "1794",
            "1795",
            "1796",
            "1797",
            "1798",
            "1799",
            "1800",
            "1801",
            "1802",
            "1803",
            "1804",
            "1805",
            "1806",
            "1807",
            "1808",
            "1809",
            "1810",
            "1811",
            "1812",
            "1813",
            "1814",
            "1815",
            "1816",
            "1817",
            "1818",
            "1819",
            "1820",
            "1821",
            "1822",
            "1823",
            "1824",
            "1825",
            "1826",
            "1827",
            "1828",
            "1829",
            "1830",
            "1831",
            "1832",
            "1833",
            "1834",
            "1835",
            "1836",
            "1837",
            "1839",
            "1840",
            "1841",
            "1842",
            "1843",
            "1844",
            "1845",
            "1846",
            "1847",
            "1848",
            "1849",
            "1850",
            "1851",
            "1852",
            "1853",
            "1854",
            "1855",
            "1856",
            "1858",
            "1859",
            "1860",
            "1861",
            "1862",
            "1863",
            "1864",
            "1865",
            "1866",
            "1867",
            "1868",
            "1869",
            "1870",
            "1871",
            "1872",
            "1873",
            "1874",
            "1875",
            "1876"
        ],
        "category": {
            "value": "refrigerator-freezer"
        },
        "goods-business-type": {
            "value": "all"
        }
    },
    "last-post-date": last_post_date
}
    response = requests.post(scroll_api_url, json=payload)#, headers=headers)
    data = response.json()
    last_post_date = data['last_post_date']

    for widget in data['web_widgets']['post_list']:
        token = widget['data']['token']
        title = widget['data']['title']
        status = widget['data']['top_description_text']
        price = widget['data']['middle_description_text']
        store = widget['data']['red_text']

        print('token =' , token, 'title =' ,title , 'status :' , status , 'price :' , price)
        cur.execute('INSERT INTO tokens_T(token_C, title_C, status_C, price_C, store_C) VALUES (?, ?, ?, ?, ?)',
            (widget['data']['token'], widget['data']['title'], widget['data']['top_description_text'], widget['data']['middle_description_text'], widget['data']['red_text']))

        conn.commit()

    #cur.close()
x = input('x :')
