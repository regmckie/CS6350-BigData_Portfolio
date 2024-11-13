# CS6350-BigData_Portfolio
About This is a portfolio for CS 6350.0U1 (Big Data Management and Analysis). This portfolio is for the Summer 2024 semester at the University of Texas at Dallas, taught by Anurag Nagar.

### Assignment 1: WordCount for Named Entities & Search Engine for Movie Plot Summaries
In the _WordCount or Named Entities_ part, you will compute the word frequency for named entities in a large file.

The steps for this part of the assignment are as follows:
  1. Find a large text file from the Gutenberg project: https://www.gutenberg.org and upload it
  to your Databricks cluster.
  2. Use a NLP library to extract only the named entities from the text.
  3. Write code for a mapreduce program that performs wordcount on the extracted named entities.
  4. The output from the map task should be in the form of (key, Value) where key is the named
  entity, and value is its count (i.e. once every time it occurs)
  5. The output from the reducer should be sorted in descending order of count. That is, the named
  entity that is most frequent should appear at the top.

In the _Search Engine for Movie Plot Summaries_ part, we will work with a dataset of movie plot summaries that is available from the
Carnegie Movie Summary Corpus site. We are interested in building a search engine for the plot
summaries that are available in the file “plot summaries.txt” that is available under the Dataset link
of the above page. You will use the tf-idf technique studied in class to accomplish the above task.

The steps for this part of the assignment are as follows:
  1. Extract and upload the file plot summaries.txt from http://www.cs.cmu.edu/~ark/personas/
  data/MovieSummaries.tar.gz to Databricks. Also upload a file containing user’s search terms
  one per line.
  2. You will need to remove stopwords by a method of your choice.
  3. You will create a tf-idf for every term and every document (represented by Wikipedia movie ID)
  using the MapReduce method.
  4. Read the search terms from the search file and output following:
    (a) User enters a single term: You will output the top 10 documents with the highest tf-idf
    values for that term.
    (b) User enters a query consisting of multiple terms: An example could be “Funny
    movie with action scenes”. In this case, you will need to evaluate cosine similarity between
    the query and all the documents and return top 10 documents having the highest cosine
    similarity values.
  5. You can display the output of your program on the screen

  For the search terms entered by the user, you will return the list of movie names sorted by
  their relevance values in descending order. Note again, that you have to return movie names,
  and not movie ID. You would need to use the movie.metadata.tsv file to lookup the movie names.

The folder for the assignment is located [here.](https://github.com/regmckie/CS6350-BigData_Portfolio/tree/main/Assignment%201)

### Assignment 2: Friend Recommendation using Mutual Friends & Implementing Naive Bayes Classifier using Spark MapReduce
In the _Friend Recommendation using Mutual Friends_ part, you will use Spark based MapReduce algorithm to generate friend
recommendation for users. The recommendations will be based on number of mutual friends. For
example, if users X and Y are not yet friends but have a large number of mutual friends, then we
may be able to recommend them to each other.

The steps for this part of the assignment are as follows:
  1. You have to come up with the best algorithm using MapReduce. You are free to use RDDs
  or DataFrames for implementation in Apache Spark. You should detail your algorithm and its
  pseudo-code in a separate file.
  2. You cannot use any external library that computes these values for you.
  3. You are free to use Databricks or other Spark environments. You have to ensure that the TA
  can run the code.
  4. The dataset for the project is at:
  https://an-ml.s3.us-west-1.amazonaws.com/soc-LiveJournal1Adj.txt
  5. Remember that you have to display the results for a random subset of 10 users on screen.

In the _Implementing Naive Bayes Classifier using Spark MapReduce_ part, you will implement a Naive Bayes classifier using MapReduce. You will need to apply
the classifier on a text based dataset of your choice.

The steps for this part of the assignment are as follows:
  1. Data Preprocessing: The first step would involve pre-processing a large text corpus of your
  choice using PySpark. This would involve steps such as tokenization, stemming, stop word
  removal. Remember to use a dataset that corresponds to classification problem.
  2. Splitting the dataset: Split the preprocessed dataset into a training set and a testing set.
  3. Training the Naive Bayes model: Implement the Naive Bayes algorithm in PySpark using
  MapReduce to train the model on the training set.
  4. Testing the model: Use the trained model to classify the documents in the testing set and
  evaluate the performance of the model.

The folder for the assignment is located [here.](https://github.com/regmckie/CS6350-BigData_Portfolio/tree/main/Assignment%202)

### Assignment 3: Spark Streaming with Real-Time Data and Kafka & Analyzing Social Networks using GraphX/GraphFrame
In the _Spark Streaming with Real-Time Data and Kafka_ part, you will create a Spark Streaming application that will continuously read text data from
a real time source, analyze the text for named entities, and send their counts to Apache Kafka. A
pipeline using Elasticsearch and Kibana will read the data from Kafka and analyze it visually.

The steps for this part of the assignment are as follows:
  1. Create a Python application that reads from a real-time data source, such as the ones mentioned
  in the previous part. Some libraries have a streaming version that continuously fetch real-time
  data. For others, you might have to write a loop that gets real time data at periodic intervals.
  2. This incoming data should continuously be written to a Kafka topic (let’s call it topic1 for
  illustration).
  3. Create a PySpark structured streaming application that continuously reads data from the Kafka
  topic (topic1) and keeps a running count of the named entities being mentioned. You should
  already know how to extract named entities from text. In this part, you will keep a running
  count of the named entities being mentioned.
  4. At the trigger time, a message containing the named entities and their counts should be sent to
  another Kafka topic (let’s call it topic2 for illustration).
  5. Configure Logstash, Elasticsearch, and Kibana to read from the Kafka topic (topic2) and create
  a bar plot of the top 10 most frequent named entities and their counts.

In the _Analyzing Social Networks using GraphX/GraphFrame_ part, you will use Spark GraphX/GraphFrame to analyze social network data. You are free to
choose any one of the Social network datasets available from the SNAP repository. You will use this dataset to construct a GraphX/GraphFrame graph and run some queries 
and algorithms on the graph.

The queries for this part of the assignment are:
  1. Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing
  edges in each
  2. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
  in each
  3. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank
  values. You are free to define any suitable parameters.
  4. Run the connected components algorithm on it and find the top 5 components with the largest
  number of nodes.
  5. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the
  largest triangle count. In case of ties, you can randomly select the top 5 vertices.

The folder for the assignment is located [here.](https://github.com/regmckie/CS6375-ML_Portfolio/tree/main/Assignment%201)

### Project
The project has two key components to it: (1) Understanding a recent machine learning technique and associated algorithm(s) and (2) Implement and apply it to a standard dataset of sufficient complexity. You have to code the main part of the algorithm without using any built-in library. You can use libraries for pre-processing, loading, analysis of results, etc.

For this project, I explored and implemented the PEGASUS (Pre-training with Extracted Gap-sentences for Abstractive Summarization) model to summarize a large corpus of text. More details are provided in the "Project" folder where you can read the project's own README.

The folder for the project is located [here.](https://github.com/regmckie/CS6375-ML_Portfolio/tree/main/Project)
