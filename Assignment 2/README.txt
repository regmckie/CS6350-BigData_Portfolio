FILENAME: README.txt
DUE DATE: 7/12/2024
AUTHOR:   Reg Gonzalez
EMAIL:    rdg170330@utdallas.edu (school) or regmckie@gmail.com (personal)
COURSE:   CS 6350.0u1, Summer 2024

DESCRIPTION:
Part 1 - Friend Recommendation using Mutual Friends: 
"In this part of the assignment, you will use Spark based MapReduce algorithm to generate friend
recommendation for users. The recommendations will be based on number of mutual friends. For
example, if users X and Y are not yet friends but have a large number of mutual friends, then we
may be able to recommend them to each other."

Part 2 - Implementing Naive Bayes Classifier using Spark MapReduce:
"In this part, you will implement a Naive Bayes classifier using MapReduce [from scratch]. You will need to apply
the classifier on a text based dataset of your choice. Following are the suggested steps:
- Data Preprocessing
- Spliting the dataset
- Training the Naive Bayes model
- Testing the model"


HOW TO COMPILE AND RUN:
You can compile and run this program through a Databricks notebook; however, since I am submitting a 
public link to my Databricks notebook, the results of the program will already be displayed. Both
parts 1 and 2 were done in the same notebook; they are separated by a headers clearly indicating which
code belongs to part 1 and which code belongs to part 2.

Pseudocode and discussion of results can be found in the documents "Assignment 2, Part 1 - Algorithm and Pseudocode"
and "Assignment 2, Part 2 - Report."


DATASET USED:
Part 1: https://an-ml.s3.us-west-1.amazonaws.com/soc-LiveJournal1Adj.txt
Part 2: https://an-utd-python.s3.us-west-1.amazonaws.com/yelp_labelled.txt

For part 1, I uploaded the dataset to the DBFS in Databricks and read it through there.
For part 2, as per the instructions, I publicly hosted the dataset through my GitHub page.