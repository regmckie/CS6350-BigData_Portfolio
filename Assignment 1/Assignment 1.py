# Databricks notebook source
# CS 6350.0U1

# Assignment 1

# Name: Reg Gonzalez
# NetID: rdg170330

# COMMAND ----------

# -----------------------------------------------------------
# |         PART 1: WordCount for Named Entities            |
# -----------------------------------------------------------

# THE FOLLOWING WAS USED TO INSTALL NLTK:
%pip install nltk
import nltk 
nltk.download("all")
from nltk import word_tokenize, ne_chunk, pos_tag

# read in book and join it into a single string so that way it's compatible with 
# NLTK methods
witchcraft_rdd = sc.textFile("/FileStore/the_discoverie_of_witchcraft.txt")
witchcraft_text = " ".join(witchcraft_rdd.collect())

# tokenize, perform POS tagging, and do Named Entity Recognition
words = word_tokenize(witchcraft_text)
tagged_words = pos_tag(words)
named_entities = ne_chunk(tagged_words)

# create a list of the named entities
list_of_named_entities = []
for entity in named_entities:
    if hasattr(entity, 'label'):
        list_of_named_entities.append(' '.join(e[0] for e in entity))

# convert back to RDD so that we can perform MapReduce functions on it
named_entities_rdd = sc.parallelize(list_of_named_entities)

# perform word count on named entities and display them 
named_entities_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).sortBy(lambda x: -x[1]).collect()


# COMMAND ----------

# --------------------------------------------------------------------
# |        PART 2: Search Engine for Movie Plot Summaries            |
# --------------------------------------------------------------------

import math
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from numpy import dot
from numpy.linalg import norm
from collections import defaultdict

# read in the plot summaries
plot_summaries = sc.textFile("/FileStore/plot_summaries.txt")

# remove stopwords from plot summaries
set_of_stopwords = set(stopwords.words("english"))

def remove_stopwords(document):
    movie_id, plot_summary = document.split("\t", 1)    # get the movie ID and plot summary (separated by a '\t' character)
    plot_summary_lowered = plot_summary.lower()
    words = word_tokenize(plot_summary_lowered)
    words_without_stopwords = [w for w in words if w.isalnum() and w not in set_of_stopwords]
    return (movie_id, words_without_stopwords)

new_plot_summaries = plot_summaries.map(remove_stopwords)

# calculate term frequencies and return the TF of the term in the document
# this will be done for all terms in all documents
def calculate_tf(movie_id, plot_summaries):
    term_counter = {}
    num_of_terms = len(plot_summaries)
    for term in plot_summaries:
        if term in term_counter:
            # if term exists in term_counter, then increment every time the term is encountered
            term_counter[term] = term_counter[term] + 1 
        else:
            # otherwise, the term will be added to term_counter
            term_counter[term] = 1  
    tf = {term: count / num_of_terms for term, count in term_counter.items()}
    return [(term, (movie_id, term_freq)) for term, term_freq in tf.items()]

# calculate TF (term frequencies), DF (document frequencies), and IDF (inverse document frequencies)
num_of_summaries = plot_summaries.count()   # here, each summary is a "document"
tf = new_plot_summaries.flatMap(lambda x: calculate_tf(x[0], x[1]))
df = tf.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x+y)
idf = df.map(lambda x: (x[0], math.log(num_of_summaries / x[1])))

# calculate TF-IDF by joining TF and IDF
tf_idf = tf.join(idf).map(lambda x: (x[1][0][0], (x[0], x[1][0][1] * x[1][1])))

# group by movie ID
tf_idf_by_movie_id = tf_idf.groupByKey()
tf_idf_by_movie_id = tf_idf_by_movie_id.mapValues(dict)

# return TF-IDFs as a dictionary of (movie id, term, TF-IDF score)
tf_idf_results = tf_idf_by_movie_id.collectAsMap()

# COMMAND ----------

# read in the movie metadata file
movie_metadata = sc.textFile("/FileStore/movie_metadata.tsv")

# we just want the movie ID and the movie name
movie_names = movie_metadata.map(lambda x: ((x.split("\t"))[0], (x.split("\t"))[2])).collectAsMap()

# get the user's search terms from the search file
user_search_terms = sc.textFile("/FileStore/user_search_file.txt")

# the first 5 lines are single-term requests and the last 5 lines are multiple-term requests
all_terms = user_search_terms.collect()
single_terms = user_search_terms.take(5)
multiple_terms = all_terms[-5:]

# calculate TF-IDF scores for the single_terms and get the top 10 movies with the highest TF-IDF scores
def single_term_top_10_movies(term, tf_idf_results):
    movie_tf_idfs = [(movie_id, tf_idf.get(term, 0)) for movie_id, tf_idf in tf_idf_results.items()]
    sorted_tf_idfs = sorted(movie_tf_idfs, key=lambda x: x[1], reverse=True)
    top_10_movies = sorted_tf_idfs[:10]
    return [(movie_names.get(movie_id, "[unknown]"), tf_idf) for movie_id, tf_idf in top_10_movies]

# display the top 10 movies for each (single) search term
for st in single_terms:
    top_10_movies_and_scores = single_term_top_10_movies(st.lower(), tf_idf_results)
    print(f"Top 10 movies for '{st}':")
    for movie_and_score in top_10_movies_and_scores:
        print(f"\tMovie Name: {movie_and_score[0]}, TF-IDF: {movie_and_score[1]}")

# COMMAND ----------

# turn each entry in multiple-terms into vectors
# we're doing this because we want to use cosine similarity to compare the vectors to the plot summaries documents
def create_multiple_term_vectors(mt_query, idf_map):
    mt_count = defaultdict(int)
    words = word_tokenize(mt_query.lower())
    mt_query_without_stopwords = [w for w in words if w not in set_of_stopwords and w.isalnum()]
    total_terms = len(mt_query_without_stopwords)

    # keep track of terms in the multiple-term query 
    for term in mt_query_without_stopwords:
        mt_count[term] = mt_count[term] + 1

    tf_of_mt_query = {term: count / total_terms for term, count in mt_count.items()}
    tf_idf_of_mt_query = {term: tf_of_mt_query[term] * idf_map.get(term, 0) for term in tf_of_mt_query}
    return tf_idf_of_mt_query

idf_map = idf.collectAsMap()
vectors_of_mt_queries = {query: create_multiple_term_vectors(query, idf_map) for query in multiple_terms}

# calculate cosine similarity
# we're comparing the multiple-term query vectors to the plot summaries documents
# we'll use these calculations to see which movies are the best match for our multiple-term queries
def calculate_cosine_similarity(vector1, vector2):
    intersection_of_terms = set(vector1.keys()).intersection(set(vector2.keys()))
    magnitude_of_vector1 = norm(list(vector1.values()))
    magnitude_of_vector2 = norm(list(vector2.values()))
    cosine_sim = (sum([vector1[term] * vector2[term] for term in intersection_of_terms])) / (magnitude_of_vector1 * magnitude_of_vector2)
    return cosine_sim

# use cosine similarity to find the top 10 documents for each multi-term query and display them
for mt_query in multiple_terms:
    mt_query_vector = vectors_of_mt_queries[mt_query]
    movie_similarities = [(movie_id, calculate_cosine_similarity(mt_query_vector, tf_idf)) for movie_id, tf_idf in tf_idf_results.items()]
    top_10_movies = sorted(movie_similarities, key=lambda x: x[1], reverse=True)[:10]
    print(f"Top 10 movies for '{mt_query}':")
    for movie_and_cosine_sim in top_10_movies:
        print(f"\tMovie Name: {movie_names.get(movie_and_cosine_sim[0], '[unknown]')}, Cosine Similarity: {movie_and_cosine_sim[1]}")
