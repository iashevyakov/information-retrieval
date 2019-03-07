import math

import postgresql

db = postgresql.open('pq://postgres:postgres@localhost:5432/articles')

term_text_articles = db.query(
    'SELECT q1.term_id, term_text, article_id FROM terms_list as q1 JOIN (SELECT * from article_term) as q2 ON q1.term_id = q2.term_id;')

term_articles_count = db.prepare('SELECT COUNT(article_id) FROM article_term WHERE term_id = $1')

term_freq_in_article = db.prepare('SELECT COUNT(*) FROM words_porter WHERE term = $1 AND article_id= $2')
terms_quantity_of_article = db.prepare('SELECT COUNT(*) FROM words_porter WHERE article_id = $1')
update_article_term_tf_idf = db.prepare('UPDATE article_term SET tf_idf = $1 WHERE article_id=$2 AND term_id=$3')

for term_text_article in term_text_articles:
    term_id, term_text, article_id = term_text_article['term_id'], term_text_article['term_text'], term_text_article[
        'article_id']
    articles_of_term_count = term_articles_count(term_id)[0][0]
    idf = math.log(30 / articles_of_term_count)
    tf = float(term_freq_in_article(term_text, article_id)[0][0]) / terms_quantity_of_article(article_id)[0][0]
    tf_idf = tf * idf
    update_article_term_tf_idf(tf_idf, article_id, term_id)