import math

import nltk
import postgresql
from nltk.corpus import stopwords

db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')

tokenizer = nltk.RegexpTokenizer(r'\S+')

porter_stemmer = nltk.SnowballStemmer('russian')

N = 30

term_articles_count = db.prepare(
    'SELECT COUNT(article_id) FROM ((SELECT * FROM article_term) AS q1 JOIN (SELECT term_id FROM terms_list WHERE term_text = $1) AS q2 ON q1.term_id=q2.term_id)')

term_urls_query = db.prepare(
    "SELECT url FROM ((SELECT article_id FROM ((SELECT term_id FROM terms_list WHERE term_text = $1) AS q1 JOIN (SELECT * FROM article_term) AS q2 ON q1.term_id=q2.term_id)) AS q3 JOIN (SELECT id,url FROM articles) AS q4 on q3.article_id=q4.id)")
document_length = db.prepare(
    "SELECT COUNT(*) FROM ((SELECT id FROM articles WHERE url=$1) AS q1 JOIN (SELECT article_id, term FROM words_porter) AS q2 on q1.id=q2.article_id);")
term_count_in_document = db.prepare(
    "SELECT COUNT(*) FROM ((SELECT id FROM articles WHERE url=$1) AS q1 JOIN (SELECT article_id, term FROM words_porter WHERE term=$2) AS q2 on q1.id=q2.article_id);")

avgdl = db.query("SELECT COUNT(*) FROM words_porter;")[0][0] / N


def preprocess(text):
    words = tokenizer.tokenize(text.lower())
    filtered_words = [word for word in words if word not in stopwords.words('russian')]
    filtered_porter_words = [porter_stemmer.stem(word) for word in filtered_words]
    return filtered_porter_words


def union(urls1, urls2):
    return set(urls1).union(set(urls2))



def bm25(query_terms, url):
    k_1, b = 1.2, 0.75
    document_len = document_length(url)[0][0]
    sum = 0

    for term in query_terms:
        term_doc_count = term_articles_count(term)[0][0]
        idf = math.log((N - term_doc_count + 0.5) / (term_doc_count + 0.5))
        term_count = term_count_in_document(url, term)[0][0]
        addend = idf * term_count * (k_1 + 1) / (term_count + k_1 * (1 - b + b * document_len / avgdl))
        if addend > 0:
            sum += addend
    return sum


def get_docs(query):
    word_articles = {}
    query_words = preprocess(query)
    for query_word in query_words:
        if term_urls_query(query_word):
            word_articles[query_word] = [term_url['url'] for term_url in term_urls_query(query_word)]
    query_terms = list(word_articles.keys())
    sorted_word_articles = sorted(word_articles.items(), key=lambda item: len(item[1]))
    result = sorted_word_articles[0][1]
    for i in range(1, len(sorted_word_articles)):
        result = union(result, sorted_word_articles[i][1])
    return list(result), query_terms


def handle_request(query):
    urls, query_terms = get_docs(query)
    bm25_values = {}
    for url in urls:
        bm25_values[url] = bm25(query_terms, url)

    sorted_articles = dict(sorted(bm25_values.items(), key=lambda item: item[1], reverse=True))
    for url, bm25_value in list(sorted_articles.items())[:10]:
        print(url, bm25_value, sep=' : ')


handle_request("проиграли на выезде")
