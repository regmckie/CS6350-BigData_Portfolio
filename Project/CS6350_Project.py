# Databricks notebook source
# CS 6350.0U1

# Project - Abstract Text Summarization using PySpark and PEGASUS

# Name: Reg Gonzalez
# NetID: rdg170330

# COMMAND ----------

# Install necessary libraries
%pip install transformers torch
%pip install sentencepiece
%pip install requests
%pip install rouge-score

# COMMAND ----------

from pyspark.sql.functions import PandasUDFType, pandas_udf, concat_ws, col
from pyspark.sql import SparkSession
from pyspark.sql import Row
from rouge_score import rouge_scorer
import pandas as pd
import requests

# Start the Spark session
spark = SparkSession.builder \
    .appName("AbstractTextSummarizationWithPEGASUS") \
    .getOrCreate()

# COMMAND ----------

# ********************************************************************************
# * SUMMARIZING 'LETTERS ON DEMONOLOGY AND WITCHCRAFT' BY WALTER SCOTT           *
# ********************************************************************************

# COMMAND ----------

# Get the text file from GitHub
demons_and_witches_url = "https://raw.githubusercontent.com/regmckie/CS6350-BigData_Portfolio/main/letters_of_demonology_and_witchcraft_1.txt"
demons_and_witches_response = requests.get(demons_and_witches_url)
demons_and_witches_text = demons_and_witches_response.text

# Create an RDD and then convert it to a DataFrame
demons_and_witches_rdd = spark.sparkContext.parallelize(demons_and_witches_text.split("\n"))
demons_and_witches_df = demons_and_witches_rdd.map(lambda x: Row(x)).toDF(["value"])

# Combine all text into a single string
demons_and_witches_text_combined = demons_and_witches_df.select(concat_ws(" ", col("value")).alias("text")).collect()[0]["text"]

# Divide the book text into smaller chunks so that it's easier to process
size_of_text_chunk = 1024  # The max input length of a chunk size in PEGASUS is 1024 tokens
demon_and_witches_book_chunks = [demons_and_witches_text_combined[counter: counter + size_of_text_chunk] for counter in range(0, len(demons_and_witches_text_combined), size_of_text_chunk)]

# COMMAND ----------

from transformers import PegasusTokenizer, PegasusForConditionalGeneration

name_of_model = "google/pegasus-xsum"   # Pick the model (PEGASUS)
p_tokenizer = PegasusTokenizer.from_pretrained(name_of_model)    # Load the pre-trained tokenizer
p_model = PegasusForConditionalGeneration.from_pretrained(name_of_model) # Define the model 

# Use the book chunks to create a DataFrame
daw_text_chunks_df = spark.createDataFrame([(book_chunk,) for book_chunk in demon_and_witches_book_chunks], ["text"])

# Since we're using Spark, broadcast the tokenizer and model to each worker
tokenizer_broadcast = spark.sparkContext.broadcast(p_tokenizer)
model_broadcast = spark.sparkContext.broadcast(p_model)

# Method to summarize book chunks
def summarize_book_chunks(book_chunks):
    tokenizer = tokenizer_broadcast.value
    model = model_broadcast.value
    list_of_summaries = []
    # Summarize each book chunk and append them to a list
    for book_chunk in book_chunks:
        inputs_for_model = tokenizer(book_chunk, return_tensors="pt", truncation=True, max_length=1024)
        ids_for_summaries = model.generate(inputs_for_model["input_ids"], early_stopping=True, length_penalty=2.0, num_beams=5, min_length=50, max_length=150)
        book_chunk_summary = tokenizer.decode(ids_for_summaries[0], skip_special_tokens=True)
        list_of_summaries.append(book_chunk_summary)
    return pd.Series(list_of_summaries)

# Register the user-defined function (UDF) in Spark
@pandas_udf("string", PandasUDFType.SCALAR)
def text_summarization_udf(book_chunks: pd.Series) -> pd.Series:
    return summarize_book_chunks(book_chunks)

# COMMAND ----------

# Apply our user-defined function to 'Letters of Demonology and Witchcraft'
demons_and_witches_summary_df = daw_text_chunks_df.withColumn("summary", text_summarization_udf(daw_text_chunks_df["text"]))

# Afterwards, collect the summaries and create a list of them
demons_and_witches_summaries = demons_and_witches_summary_df.select("summary").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# Premade reference summary:
# http://www.walterscott.lib.ed.ac.uk/works/prose/witchcraft.html#:~:text=Back%20to%20top-,Synopsis,period%20to%20his%20own%20day.
daw_reference_summary = ["The book takes the form of ten letters addressed to Lockhart, the epistolary mode permitting Scott to be both conversational in tone and discursive in method. In these, Scott surveys opinions respecting demonology and witchcraft from the Old Testament period to his own day."]

# Calculate ROUGE scores as an evaluation metric
# This compares two different pieces of text: the reference summary and the summary generated by PEGASUS
def calculate_rouge_score(chunk_summary, reference_sum):
    rs = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    rouge_scores = rs.score(reference_sum, chunk_summary)
    return rouge_scores

# Calculate ROUGE scores for each book chunk summary
rouge_scores = [calculate_rouge_score(chunk_summary, reference_sum) for reference_sum, chunk_summary in zip(daw_reference_summary, demons_and_witches_summaries)]

# COMMAND ----------

# Print summary for 'Letters of Demonology and Witchcraft'
print("\nSummary for 'Letters of Demonology and Witchcraft':")
for summary in demons_and_witches_summaries:
    print(summary)

# COMMAND ----------

# Print ROUGE scores for 'Letters on Demonology and Witchcraft'
for counter, r_scores in enumerate(rouge_scores):
    print(f"\nROUGE scores for 'Letters on Demonology and Witchcraft' summary:")
    print(r_scores)

# COMMAND ----------

# *****************************************************************
# * SUMMARIZING 'JOKER: FOLIE A DEUX' WIKIPEDIA ARTICLE           *
# *****************************************************************

# COMMAND ----------

joker_text = [
    ("Joker: Folie a Deux is an upcoming American musical psychological thriller film directed by Todd Phillips from a screenplay co-written with Scott Silver. The sequel to Joker (2019), loosely based on DC Comics characters, Joaquin Phoenix reprises his role as the Joker, with Lady Gaga joining the cast as his love interest, Harley Quinn. Zazie Beetz also reprises her role from the previous film, while Brendan Gleeson and Catherine Keener also join the cast. It is produced by Warner Bros. Pictures and DC Studios in association with Joint Effort. Joker was conceived as a standalone film, although Warner Bros. intended for it to launch a DC Black film series. Phillips expressed interest in making a sequel, but reiterated that Joker was not set up to have one. The sequel entered development in June 2022, with Gaga and Beetz joining later that year. Principal photography took place in Los Angeles, New York City, and Belleville, New Jersey, from December 2022 to April 2023. Joker: Folie a Deux will premiere at the 81st Venice International Film Festival, where it will be competing for the Golden Lion, and is scheduled for a theatrical release overseas on October 2, 2024, followed by the United States two days later on October 4, 2024, by Warner Bros. Pictures. After murdering Murray Franklin live on television, Arthur Fleck is incarcerated in Arkham State Hospital, where he meets music therapist Lee. The two fall madly in love and experience musical madness through their shared delusions, while Fleck's followers start a movement to free him from Arkham, ultimately giving rise to the Clown Prince of Crime's criminal empire. Joaquin Phoenix as Arthur Fleck / The Joker, a mentally ill, nihilistic criminal with a clown-inspired persona, formerly an impoverished party clown and aspiring stand-up comedian. Lady Gaga as Lee / Harley Quinn, a music therapist for Arkham Asylum who meets Arthur; her curiosity eventually turns to obsession and she forms a deadly romantic relationship with him. Zazie Beetz as Sophie Dumond, a single mother and Arthur's former neighbor, whom Arthur imagined being in a romantic relationship with. Harry Lawtey as Harvey Dent, Gotham City's District Attorney who plans to prosecute Arthur for his crimes. Leigh Gill and Sharon Washington reprise their roles as Gary and Arthur's social worker, respectively. Brendan Gleeson, Catherine Keener, Jacob Lofland, Steve Coogan, and Ken Leung have been cast as in undetermined roles.",)
]

# Create a DataFrame based on the Wikipedia text
joker_df = spark.createDataFrame(joker_text, ["text"])

# COMMAND ----------

from transformers import PegasusTokenizer, PegasusForConditionalGeneration

name_of_model = "google/pegasus-xsum"   # Pick the model (PEGASUS)
p_tokenizer = PegasusTokenizer.from_pretrained(name_of_model)    # Load the pre-trained tokenizer
p_model = PegasusForConditionalGeneration.from_pretrained(name_of_model) # Define the model 

# Since we're using Spark, broadcast the tokenizer and model to each worker
tokenizer_broadcast = spark.sparkContext.broadcast(p_tokenizer)
model_broadcast = spark.sparkContext.broadcast(p_model)

# Redefine the summarize_book_chunks function to work with Joker text
def summarize_book_chunks(book_chunks):
    list_of_summaries = []
    tokenizer = tokenizer_broadcast.value
    model = model_broadcast.value
    for book_chunk in book_chunks:
        inputs_for_model = tokenizer(book_chunk, return_tensors="pt", truncation=True, max_length=1024)
        ids_for_summaries = model.generate(inputs_for_model["input_ids"], early_stopping=True, length_penalty=2.0, num_beams=5, max_length=150)
        book_chunk_summary = tokenizer.decode(ids_for_summaries[0], skip_special_tokens=True)
        list_of_summaries.append(book_chunk_summary)
    return pd.Series(list_of_summaries)

# Register the user-defined function (UDF) in Spark
@pandas_udf("string", PandasUDFType.SCALAR)
def text_summarization_udf(book_chunks: pd.Series) -> pd.Series:
    return summarize_book_chunks(book_chunks)

# COMMAND ----------

# Apply our user-defined function to 'Joker: Folie a Deux'
joker_summary_df = joker_df.withColumn("summary", text_summarization_udf(joker_df["text"]))

# Afterwards, collect the summaries and create a list of them
joker_summaries = joker_summary_df.select("summary").rdd.flatMap(lambda x: x).collect()

# COMMAND ----------

# Premade reference summary:
# https://digital.abcaudio.com/news/trailer-joker-folie-deux-teases-trial-century-joaquin-phoenixs-arthur-fleck
joker_reference_summary = ["'Joker: Folie a Deux' finds Arthur Fleck institutionalized at Arkham awaiting trial for his crimes as Joker. While struggling with his dual identity, Arthur not only stumbles upon true love, but also finds the music that's always been inside him."] 

# Calculate ROUGE scores as an evaluation metric
# This compares two different pieces of text: the reference summary and the summary generated by PEGASUS
def calculate_rouge_score(chunk_summary, reference_sum):
    rs = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)
    rouge_scores = rs.score(reference_sum, chunk_summary)
    return rouge_scores

# Compute ROUGE scores for each summary
rouge_scores = [calculate_rouge_score(chunk_summary, reference_sum) for reference_sum, chunk_summary in zip(joker_reference_summary, joker_summaries)]

# COMMAND ----------

# Print summary for 'Joker: Folie a Deux' Wikipedia article
print("\nSummary for 'Joker: Folie a Deux' Wikipedia article:")
for summary in joker_summaries:
    print(summary)

# COMMAND ----------

# Print ROUGE scores for 'Joker: Folie a Deux' Wikipedia article
for counter, r_scores in enumerate(rouge_scores):
    print(f"\nROUGE scores for 'Joker: Folie a Deux' Wikipedia article summary:")
    print(r_scores)
