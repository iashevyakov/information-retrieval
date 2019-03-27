import math

import nltk
import numpy as np
import postgresql
from nltk.corpus import stopwords

db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')

tokenizer = nltk.RegexpTokenizer(r'\S+')

porter_stemmer = nltk.SnowballStemmer('russian')

terms_query = db.query("SELECT * FROM terms_list;")

term_articles_count = db.prepare('SELECT COUNT(article_id) FROM article_term WHERE term_id = $1')

tf_idf = db.prepare(
    'SELECT tf_idf FROM ((SELECT id FROM articles WHERE url=$1) AS q1 JOIN (SELECT * FROM article_term WHERE term_id=$2) AS q2 ON q1.id=q2.article_id )')

term_urls_query = db.prepare(
    "SELECT url FROM ((SELECT article_id FROM ((SELECT term_id FROM terms_list WHERE term_text = $1) AS q1 JOIN (SELECT * FROM article_term) AS q2 ON q1.term_id=q2.term_id)) AS q3 JOIN (SELECT id,url FROM articles) AS q4 on q3.article_id=q4.id)")


def preprocess(text):
    words = tokenizer.tokenize(text.lower())
    filtered_words = [word for word in words if word not in stopwords.words('russian')]
    filtered_porter_words = [porter_stemmer.stem(word) for word in filtered_words]
    return filtered_porter_words


def union(urls1, urls2):
    return set(urls1).union(set(urls2))


def cosine_measure(d1_vector, d2_vector):
    norm1 = math.sqrt(sum([value ** 2 for value in d1_vector]))
    norm2 = math.sqrt(sum([value ** 2 for value in d2_vector]))
    cos_value = sum([value1 * value2 for value1, value2 in zip(d1_vector, d2_vector)]) / (norm1 * norm2)
    return cos_value


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
    q_T_vector = []
    A_matrix = np.empty(shape=(len(terms_query), len(urls)))
    term_index = 0
    for term in terms_query:
        if term['term_text'] in query_terms:
            idf = math.log(30 / term_articles_count(term['term_id'])[0][0])
            q_T_vector.append(idf)
        else:
            q_T_vector.append(0)
        term_articles = [term_url['url'] for term_url in term_urls_query(term['term_text'])]
        doc_index = 0
        for url in urls:
            if url in term_articles:
                term_doc_value = tf_idf(url, term['term_id'])[0][0]
            else:
                term_doc_value = 0
            A_matrix.itemset((term_index, doc_index), term_doc_value)
            doc_index += 1
        term_index += 1

    u, s, v_T = np.linalg.svd(A_matrix, full_matrices=False)
    # берем первые строки из V_T вместо первых столбцов из V (то же самое)
    u_k, s_k, v_T_k = u[:, :5], np.diag(s[:5]), v_T[:5, :]
    s_k_inv = np.linalg.inv(s_k)
    q_vector = np.dot(np.dot(np.array(q_T_vector), u_k), s_k_inv)
    cos_values = {}
    for i, url in enumerate(urls):

        cos_values[url] = cosine_measure(list(q_vector), list(v_T_k[:, i]))
    sorted_articles = dict(sorted(cos_values.items(), key=lambda item: item[1], reverse=True))
    for url, cos_value in list(sorted_articles.items())[:10]:
        print(url, cos_value, sep=' : ')


handle_request("была очень сложная игра")
