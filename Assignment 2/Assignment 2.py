# Databricks notebook source
# CS 6350.0U1

# Assignment 2

# Name: Reg Gonzalez
# NetID: rdg170330

# COMMAND ----------

# ************************************************************
# * PART 1 - FRIEND RECOMMENDATION USING MUTUAL FRIENDS      *
# ************************************************************

import random
from pyspark.sql.functions import col, collect_list, explode, lit, array_contains, dense_rank
from pyspark.sql import Window, types

# Read in the data
list_of_friends_txt = spark.read.text("/FileStore/list_of_friends.txt")
list_of_friends_rdd = list_of_friends_txt.rdd 

# Split on "\t", which separates the UserID and list of friends
filtered_data = list_of_friends_rdd.map(lambda x: x.value.split("\t"))  
filtered_data = filtered_data.filter(lambda x: x[1] and x[0] and len(x) == 2)

# Split the list of friends by ",", and filter out empty strings and convert to ints
def parse_friends(row):
    userID = int(row[0])
    friends_list = [int(friend) for friend in row[1].split(",") if friend.isdigit()]
    return (userID, friends_list) 

filtered_data = filtered_data.map(parse_friends)

# Create a dataframe based on this new filtered data
listOfFriendsDF = spark.createDataFrame(filtered_data, ["user", "list of friends"])

# Change the dataframe so that we get pairwise relationships instead of an aray of friends 
# E.g., 
# user | friend     instead of      user | list of friends
# 0      1                          0      [1, 2, 3, ...]
# 0      2
# 0      3
new_listOfFriendsDF = listOfFriendsDF.withColumn("friend", explode("list of friends"))
new_listOfFriendsDF = new_listOfFriendsDF.drop("list of friends")

# Join the df with itself so that we can start finding friend recommendations
# Then, select columns that represent the user, the friend recommendation, and the friend that they share in common
# Finally, filter out self-recommendations (i.e., the friend recommendation should not be the same as the user) 
friendRecommendationsDF = new_listOfFriendsDF.alias("friendsDF1").join(new_listOfFriendsDF.alias("friendsDF2"),
                                                                       col("friendsDF1.friend") == col("friendsDF2.user"))
friendRecommendationsDF = friendRecommendationsDF.select(col("friendsDF1.user").alias("user"),
                                                         col("friendsDF2.friend").alias("friend recommendation"),
                                                         col("friendsDF1.friend").alias("friend in common"))
friendRecommendationsDF = friendRecommendationsDF.filter(col("user") != col("friend recommendation"))

# Count the friend recommendations 
friend_rec_count = friendRecommendationsDF.groupBy("user", "friend recommendation").count()
friend_rec_count = friend_rec_count.withColumnRenamed("count", "friend recommendation count")

# Get rid of existing friends so that way we don't recommend them again
existing_friends = new_listOfFriendsDF.groupBy("user").agg(collect_list("friend").alias("existing friends"))
new_friend_recs = friend_rec_count.join(existing_friends, how="left", on="user")
new_friend_recs = new_friend_recs.filter(~array_contains(col("existing friends"), col("friend recommendation")))

# Get top 10 recommendations for each user
top_10_friend_recs = new_friend_recs.withColumn("friend rank", dense_rank().over(Window.partitionBy("user").orderBy(col("friend recommendation count").desc())))
top_10_friend_recs = top_10_friend_recs.filter(col("friend rank") <= 10).groupBy("user").agg(collect_list("friend recommendation").alias
("friend recommendations"))

# Format the output to display 
output_to_display = top_10_friend_recs.rdd.map(lambda x: f"USER ID: {x['user']}     FRIEND RECOMMENDATIONS: {','.join(map(str, x['friend recommendations'][:10]))}")
random_10_users = output_to_display.takeSample(False, 10)

# Randomly sample 10 users and display their friend recommendations
for user_and_recs in random_10_users:
    print(user_and_recs)

# COMMAND ----------

# ****************************************************************************
# * PART 2 - IMPLEMENTING NAIVE BAYES CLASSIFER USING SPARK MAPREDUCE        *
# ****************************************************************************

import numpy as np
import pandas as pd
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.ml.feature import StopWordsRemover, IDF, Tokenizer, HashingTF

spark = SparkSession.builder.appName("yelpLabelled").getOrCreate()

# Read in the input data
dataset_path = "https://raw.githubusercontent.com/regmckie/CS6350-BigData_Portfolio/main/yelp_labelled.txt"
column_names = ["text", "label"]
dataset = pd.read_csv(dataset_path, sep="\t", header=None, names=column_names)
yelpDF = spark.createDataFrame(dataset)
#display(yelpDF)

# Tokenize and remove stopwords from the text
tokenizer = Tokenizer(outputCol="words", inputCol="text")
stopwords_remover = StopWordsRemover(outputCol="words_without_stopwords", inputCol="words")
wordsDF = tokenizer.transform(yelpDF)
words_without_stopwordsDF = stopwords_remover.transform(wordsDF)
#display(words_without_stopwords)

# Convert words_without_stopwords vector to numerical vectors
hashing_tfDF = HashingTF(outputCol="numerical_words", inputCol="words_without_stopwords", numFeatures=50)
numerical_wordsDF = hashing_tfDF.transform(words_without_stopwordsDF)
#display(numerical_wordsDF)

# Create an IDF model based from the numerical_words dataframe. 
# The model takes the numerical feature vector and scales each feature; it weighs down features that occur more frequently.
idf = IDF(outputCol="features", inputCol="numerical_words")
idf_model = idf.fit(numerical_wordsDF)
new_yelpDF = idf_model.transform(numerical_wordsDF)
new_yelpDF = new_yelpDF.select("label", "features") # Take only the columns we need
#display(new_yelpDF)

# Split the data into training and testing sets
trainingDF, testingDF = new_yelpDF.randomSplit([0.80, 0.20], seed=1234)


# COMMAND ----------

# Convert the training set to a RDD for processing
trainingDF_rdd = trainingDF.rdd.map(lambda sample: (sample.label, sample.features))

# Naive Bayes formula: P(c|x) = [P(x|c) * P(c)] / P(x), where 'c' is the class and 'x' is the data

# Calculate prior probabilities, represented as P(c)
total_samples = trainingDF_rdd.count()
label_counts = trainingDF_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
priors = label_counts.map(lambda x: (x[0], x[1] / total_samples)).collectAsMap()

# Calculate likelihoods, represented as P(x|c)

# Create dictionaries that represent the sums and counts of the features
def sums_and_counts_of_features(label_and_features):
    label, features = label_and_features
    array_of_features = np.array(features.toArray())    # Convert to NumPy array for better processing
    counts_of_feature = defaultdict(float)  # Represents the count of features per class
    sums_of_feature = defaultdict(float)    # Represents the sum of features per class

    # For every feature index (i.e., 'counter'), add that feature's value to the sum and increment the count
    for counter, value in enumerate(array_of_features):
        counts_of_feature[counter] = counts_of_feature[counter] + 1
        sums_of_feature[counter] = sums_of_feature[counter] + value

    # Convert to dicts for better processing later on
    sums_of_feature = dict(sums_of_feature)
    counts_of_feature = dict(counts_of_feature)

    return (label, (sums_of_feature, counts_of_feature))

# Adds/combines of sums_of_feature and counts_of_feature from different samples
def add_sums_and_counts(x, y):
    sums_of_feature_x, counts_of_feature_x = x
    sums_of_feature_y, counts_of_feature_y = y
    added_counts = defaultdict(float, counts_of_feature_x)  # Represents the combined counts_of_feature values of different samples 
    added_sums = defaultdict(float, sums_of_feature_x)  # Represents the combined sums_of_feature values of different samples 

    # Add values from sums_of_feature of different samples
    for k, v in sums_of_feature_y.items():
        added_sums[k] = added_sums[k] + v

    # Add values from counts_of_feature of different samples
    for k, v in counts_of_feature_y.items():
        added_counts[k] = added_counts[k] + v

    # Convert to dicts for better processing later on
    added_sums = dict(added_sums)
    added_counts = dict(added_counts)

    return(added_sums, added_counts)

sums_and_counts_features = trainingDF_rdd.map(sums_and_counts_of_features).reduceByKey(add_sums_and_counts)

# Calculate likelihoods, represented as P(x|c)
feature_likelihoods = sums_and_counts_features.mapValues(lambda sums_and_counts: {counter: sums_and_counts[0][counter] / sums_and_counts[1][counter] for counter in sums_and_counts[0]}).collectAsMap()
#display(feature_likelihoods)


# COMMAND ----------

# Convert the testing set to a RDD for processing
testingDF_rdd = testingDF.rdd.map(lambda sample: (sample.label, sample.features))

# Create predictions for each sample/document
def create_predictions(features):
    # Represents log probs for each class
    log_probabilities = {class_label: np.log(prior_for_class) for class_label, prior_for_class in priors.items()}  
    array_of_features = np.array(features.toArray())    # Convert to NumPy array for better processing
    
    # For every feature, if it's > 0, then first multiply the log probability of that specific feature
    # with the value of that feature and add it to the class label's log probability
    for class_label, feature_likelihood in feature_likelihoods.items():
        for counter, value_of_feature in enumerate(array_of_features):
            if value_of_feature > 0:
                if counter in feature_likelihood:
                    log_probabilities[class_label] = log_probabilities[class_label] + value_of_feature * np.log(feature_likelihood[counter])
                else:
                    # Smoothing, if necessary
                    log_probabilities[class_label] = log_probabilities[class_label] + value_of_feature * np.log(1e-9)  
   
    # Return class label with the highest log probability
    prediction = max(log_probabilities, key=log_probabilities.get)

    return prediction

# Make predictions
all_preds = testingDF_rdd.map(lambda sample: (sample[0], create_predictions(sample[1])))

# Calculate accuracy, recall, precision, the F1 score, and the confusion matrix

# Accuracy
total_preds = all_preds.count()
correct_preds = all_preds.filter(lambda x: x[0] == x[1]).count()
accuracy = correct_preds / total_preds

# Confusion matrix
elements_in_cf = all_preds.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y).collectAsMap()
true_positive = elements_in_cf.get((1, 1), 0)
false_positive = elements_in_cf.get((0, 1), 0)
true_negative = elements_in_cf.get((0, 0), 0)
false_negative = elements_in_cf.get((1, 0), 0)

# Recall
recall = true_positive / (true_positive + false_negative) if (true_positive + false_negative) > 0 else 0

# Precision
precision = true_positive / (true_positive + false_positive) if (true_positive + false_positive) > 0 else 0

# F1 Score
f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

# Display evaluation metrics
print("Accuracy: " + str(accuracy))
print("Recall: " + str(recall))
print("Precision: " + str(precision))
print("F1 Score: " + str(f1_score))
print("Confusion Matrix: ")
print("TP: " + str(true_positive) + "    FP: " + str(false_positive))
print("FN: " + str(false_negative) + "    TN: " + str(true_negative))
