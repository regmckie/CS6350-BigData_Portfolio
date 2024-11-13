# Databricks notebook source
# CS 6350.0U1

# Assignment 3, Part 2 - Analyzing Social Networks using GraphX/GraphFrames

# Name: Reg Gonzalez
# NetID: rdg170330

# COMMAND ----------

# *******************************************************************************
# * PART 2 - ANALYZING SOCIAL NETWORKS USING GRAPHX/GRAPHFRAMES                 *
# *******************************************************************************

%pip install graphframes

# Go to the cluster you're running this notebook on -->
# Configuration -->
# Libraries -->
# Install new --> 
# Maven --> 
# Input 'graphframes:graphframes:0.8.1-spark3.0-s_2.12' into Coordinates

# COMMAND ----------

# *****************************************
# * STEP 1: LOADING DATA                  *
# *****************************************

from graphframes import GraphFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode
from pyspark.sql.functions import monotonically_increasing_id

# Start the Spark session
spark = SparkSession.builder \
    .appName("Congressional Network Analysis") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.1-spark3.0-s_2.12") \
    .getOrCreate()

# Set up a checkpoint directory for PART (3d.)
spark.sparkContext.setCheckpointDir("/FileStore/checkpoints")

# Read in the data
congress_network_path = "/FileStore/congress_network_data.json"

# Load in the JSON data
congress_network_data = spark.read.json(congress_network_path)

# Extract vertices (the names of congresspeople)
v = congress_network_data.select(posexplode("usernameList").alias("id", "name")).distinct()

# Extract edges (the directed relationships between congresspeople)
source_df = congress_network_data.select(explode("outList").alias("src"))
source_df = source_df.withColumn("src", explode("src"))

destination_df = congress_network_data.select(explode("inList").alias("dst"))
destination_df = destination_df.withColumn("dst", explode("dst"))

# Join the source_df and destination_df dataframes 
e = source_df.withColumn("username_id", monotonically_increasing_id())
e = e.join(destination_df.withColumn("username_id", monotonically_increasing_id()), "username_id")
e = e.select("src", "dst")

# Create the GraphFrame
graphframe = GraphFrame(v, e)

# Display graphframe to show that it worked
graphframe.vertices.show()
graphframe.edges.show()

# COMMAND ----------

# *****************************************
# * STEP 2: CREATE GRAPHS                 *
# *****************************************

from pyspark.sql.types import StringType, StructField, IntegerType, StructType

# Define the vertex and edge schema
vertex_schema = StructType([
    StructField("username_id", IntegerType(), True),
    StructField("name", StringType(), True)
])

edge_schema = StructType([
    StructField("src", IntegerType(), True),
    StructField("dst", IntegerType(), True)
])

# Load vertices and edges
v = v.withColumn("id", v["id"].cast(IntegerType()))
e = e.withColumn("src", e["src"].cast(IntegerType()))
e = e.withColumn("dst", e["dst"].cast(IntegerType()))

# Create the GraphFrame
graphframe = GraphFrame(v, e)

# COMMAND ----------

# ******************************************************
# * STEP 3: RUN QUERIES AND ALGORITHMS                 *
# ******************************************************

# (a.) Find the top 5 nodes with the highest outdegree and find the count of the number of outgoing
# edges in each

# Calculate outdegrees
outdegrees = graphframe.outDegrees

# Find top 5 nodes with highest outdegree
top_5_highest_outdegrees = outdegrees.orderBy("outDegree", ascending=False).limit(5)

# Get the names of the congresspeople associated with the IDs
top_5_highest_outdegrees_with_names = top_5_highest_outdegrees.join(v, top_5_highest_outdegrees.id == v.id)
top_5_highest_outdegrees_with_names = top_5_highest_outdegrees_with_names.select(v.name, top_5_highest_outdegrees.outDegree)

# Write the top 5 highest outdegrees to a file
top_5_highest_outdegrees_with_names.write.csv("/FileStore/top_5_highest_outdegrees.csv")

# Show the top 5 highest outdegrees
top_5_highest_outdegrees_with_names.show()


# COMMAND ----------

# (b.) Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
# in each

# Calculate indegree
indegrees = graphframe.inDegrees

# Find top 5 nodes with highest indegree
top_5_highest_indegrees = indegrees.orderBy("inDegree", ascending=False).limit(5)

# Get the names of the congresspeople associated with the IDs
top_5_highest_indegrees_with_names = top_5_highest_indegrees.join(v, top_5_highest_indegrees.id == v.id)
top_5_highest_indegrees_with_names = top_5_highest_indegrees_with_names.select(v.name, top_5_highest_indegrees.inDegree)

# Write the top 5 highest indegrees to a file
top_5_highest_indegrees_with_names.write.csv("/FileStore/top_5_highest_indegrees.csv")

# Show the top 5 highest indegrees
top_5_highest_indegrees_with_names.show()

# COMMAND ----------

# (c.) Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank
# values. You are free to define any suitable parameters.

# Perform PageRank
pagerank_results = graphframe.pageRank(resetProbability=0.15, maxIter=10)

# Find top 5 nodes with highest PageRank values
top_5_highest_pageranks = pagerank_results.vertices.orderBy("pagerank", ascending=False).limit(5)

# Write the top 5 highest PageRank values to a file
top_5_highest_pageranks.write.csv("/FileStore/top_5_highest_pageranks.csv")

# Show the top 5 highest PageRank values
top_5_highest_pageranks.show()

# COMMAND ----------

# (d.) Run the connected components algorithm on it and find the top 5 components with 
# the largest number of nodes.

# Perform the connected components algorithm
connected_components = graphframe.connectedComponents()

# Find top 5 components with largest number of nodes
connected_component_counts = connected_components.groupBy("component").count()
top_5_components = connected_component_counts.orderBy("count", ascending=False).limit(5)

# Write the top 5 components with the largest number of nodes to a file
top_5_components.write.csv("/FileStore/top_5_components.csv")

# Show the top 5 components with the largest number of nodes 
top_5_components.show()

# COMMAND ----------

# (e.) Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the
# largest triangle count. In case of ties, you can randomly select the top 5 vertices. 

# Perform the triangle counts algorithm
triangle_counts = graphframe.triangleCount()

# Find top 5 vertices with largest triangle count
top_5_triangle_counts = triangle_counts.orderBy("count", ascending=False).limit(5)

# Write the top 5 vertices with the largest triangle count to a file
top_5_triangle_counts.write.csv("/FileStore/top_5_triangle_counts.csv")

# Show the top 5 vertices with the largest triangle count
top_5_triangle_counts.show()

