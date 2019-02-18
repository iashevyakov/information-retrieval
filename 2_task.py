import postgresql
import nltk
from nltk.corpus import stopwords
from pymystem3 import Mystem
from nltk.stem.snowball import SnowballStemmer


db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')

articles = db.query("SELECT * FROM articles;")

mystem = Mystem()
porter_stemmer = SnowballStemmer('russian')

insert_porter = db.prepare("INSERT INTO words_porter (term, article_id) VALUES ($1, $2)")
insert_mystem = db.prepare("INSERT INTO words_mystem (term, article_id) VALUES ($1, $2)")

for article in articles:
    id = article['id']
    title, keywords, content = article['title'], article['keywords'], article['content']
    text = title + keywords + content
    text = text.lower()
    tokenizer = nltk.RegexpTokenizer(r'\w+')
    words = tokenizer.tokenize(text)
    filtered_words = [word for word in words if word not in stopwords.words('russian')]
    filtered_mystem_words = [word for word in mystem.lemmatize(' '.join(filtered_words)) if word not in [' ', '\n']]
    filtered_porter_words = [porter_stemmer.stem(word) for word in filtered_words]
    for term in filtered_porter_words:
        insert_porter(term, id)
    for term in filtered_mystem_words:
        insert_mystem(term, id)
