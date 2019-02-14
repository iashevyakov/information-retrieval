import postgresql
from lxml import html
import requests
from w3lib.html import replace_entities

db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')
ins = db.prepare("INSERT INTO articles (title, keywords, content, url, student_id) VALUES ($1, $2, $3, $4, $5)")
STUDENT_ID = 118
page = requests.get('https://www.championat.com/news/volleyball/1.html')
tree = html.fromstring(page.content)

links = tree.xpath('//div[@class="news-item__content"]/a[@data-event-action="click_news"]/@href')[:30]
URL = 'https://www.championat.com'
for link in links:
    article_page = requests.get(URL + link)
    article_tree = html.fromstring(article_page.content)
    title = article_tree.xpath('//div[@id="article_head_title"]/text()')[0]
    title = title.strip()
    p_nodes = article_tree.xpath('//div[@id="article_content"]/p')
    content = []
    for p in p_nodes:
        p_text = p.xpath('descendant-or-self::*/text()')
        text = ''.join(p_text)
        content.append(text)
    content = replace_entities('\n'.join(content))
    keywords = article_tree.xpath('//a[@class="tags__item js-tags-item"]/span/text()')
    keywords = ';'.join(keywords)
    ins(title, keywords, content, URL + link, STUDENT_ID)


