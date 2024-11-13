FILENAME: README_Assignment3-Part2.txt (this README is for Assignment 3, Part 1 - Analyzing Social Networks using GraphX/GraphFrame)
DUE DATE: 7/26/2024
AUTHOR: Reg Gonzalez
EMAIL: rdg170330@utdallas.edu (school) or regmckie@gmail.com (personal)
COURSE: CS 6350.0u1, Summer 2024


DESCRIPTION:
"In this part, you will use Spark GraphX/GraphFrame to analyze social network data. You are free to
choose any one of the Social network datasets available from the SNAP repository.

You will use this dataset to construct a GraphX/GraphFrame graph and run some queries and algo-
rithms on the graph."


HOW TO COMPILE AND RUN:
You can compile and run this program through a Databricks notebook; however, since I am submitting a 
public link to my Databricks notebook, the results of the program will already be displayed. 

PUBLIC LINK TO DATABRICKS NOTEBOOK: https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3067718369433077/741245103989331/6045468193687166/latest.html

If you choose to run the notebook yourself, you need to install a library for your cluster. 
Follow these instructions: 
1. Go to the cluster you're running this notebook on
2. Go to Configuration
3. Go to Libraries
4. Go to Install new
5. Go to Maven
6. Input "graphframes:graphframes:0.8.1-spark3.0-s_2.12" into Coordinates

There is a report that shows screenshots of the results of each query and includes a summary
and analysis of the results. The report is called "Assignment 3, Part 2 - Report."


DATASET USED:
https://snap.stanford.edu/data/congress-twitter.html

"This network represents the Twitter interaction network for the 117th United Stated Congress,
both House of Representatives and Senate. The base data was collected via the Twitter's API,
then the empirical transmission probabilities were quantified according to the fraction of times
one member retweeted, quote tweeted, replied to, or mentioned another member's tweet."