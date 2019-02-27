import nltk
import postgresql
from nltk import SnowballStemmer
from nltk.corpus import stopwords

db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')

term_set = db.query("SELECT DISTINCT term FROM words_porter;")

articles_set_of_term = db.prepare("SELECT DISTINCT article_id FROM words_porter WHERE term = $1;")

insert_term = db.prepare("INSERT INTO terms_list (term_text) VALUES ($1)")

insert_article_term = db.prepare("INSERT INTO article_term (article_id, term_id) VALUES ($1, $2)")

terms_query = db.prepare("SELECT * FROM terms_list;")

# достаём сразу url у статей, содержащих определенное слово, и в дальнейшем делаем пересечения по ним, так как они тоже уникальны как и id статей.
term_urls_query = db.prepare(
    "SELECT url FROM ((SELECT article_id FROM ((SELECT term_id FROM terms_list WHERE term_text = $1) AS q1 JOIN (SELECT * FROM article_term) AS q2 ON q1.term_id=q2.term_id)) AS q3 JOIN (SELECT id,url FROM articles) AS q4 on q3.article_id=q4.id)")

porter_stemmer = SnowballStemmer('russian')

tokenizer = nltk.RegexpTokenizer(r'\S+')


def fill_database():
    for term in sorted([term['term'] for term in term_set]):
        insert_term(term)
    for term in terms_query:
        articles = articles_set_of_term(term['term_text'])
        for article in articles:
            insert_article_term(article['article_id'], term['term_id'])


def preprocess(text):
    words = tokenizer.tokenize(text.lower())
    filtered_words = [word for word in words if word not in stopwords.words('russian')]
    filtered_porter_words = [porter_stemmer.stem(word) for word in filtered_words]
    return filtered_porter_words


def intersection(urls1, urls2):
    return set(urls1).intersection(set(urls2))


def handle_request(query):
    word_articles = {}
    query_words = preprocess(query)
    for query_word in query_words:
        # берем только те слова из запроса, которые есть в коллекции
        if term_urls_query(query_word):
            word_articles[query_word] = [term_url['url'] for term_url in term_urls_query(query_word)]
    # cортировка по количеству вхождений
    sorted_word_articles = sorted(word_articles.items(), key=lambda item: len(item[1]))
    # ищем пересечение по порядку
    result = sorted_word_articles[0][1]
    for i in range(1, len(sorted_word_articles)):
        result = intersection(result, sorted_word_articles[i][1])
    for url in list(result):
        print(url)

fill_database()
handle_request("этап Лиги Чемпионов")
