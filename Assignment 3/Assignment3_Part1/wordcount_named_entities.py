from __future__ import print_function

from pyspark.sql.functions import udf, col, explode, concat_ws
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql import SparkSession
from datetime import datetime
import spacy
import sys

# Load necessary model for Named Entity Recognition
nlp = spacy.load("en_core_web_sm")

# Method that returns a list of named entities from text
def extract_named_entities(text):
    document = nlp(text)
    return [ent.text for ent in document.ents]

extract_named_entities_udf = udf(extract_named_entities, ArrayType(StringType()))

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: wordcount_named_entities.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    spark = SparkSession\
        .builder\
        .appName("NERWordcount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Create DataFrame representing the stream of input lines from Kafka
        lines = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", bootstrapServers)\
            .option(subscribeType, topics)\
            .load()\
            .selectExpr("CAST(value AS STRING)")

        # Get named entities
        named_entities = lines.withColumn("entities", extract_named_entities_udf(col("value")))

        # Explode the entities into individual rows
        exploded_named_entities = named_entities.select(explode(col("entities")).alias("entity"))

        # Get word count of named entities
        named_entity_counts = exploded_named_entities.groupBy("entity").count()
        
        # Combine the named entity and its count into a single string to send to Kafka topic 'topic2'
        result = named_entity_counts.withColumn("result", concat_ws(": ", col("entity"), col("count"))).orderBy(col("count").desc())

        # Dynamic checkpoint directory based on timestamp
        checkpoint_dir = f"/tmp/kafka_wordcount_checkpoint_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Write out the named entity and its word count to 'topic2'           
        query = result\
            .selectExpr("CAST(result AS STRING) AS value")\
            .writeStream\
            .outputMode('complete')\
            .format('kafka')\
            .option("kafka.bootstrap.servers", bootstrapServers)\
            .option("topic", "topic2")\
            .option("checkpointLocation", checkpoint_dir)\
            .start()
        
        # Print to console so you can see what was sent to 'topic2'
        query1 = named_entity_counts\
            .writeStream\
            .outputMode('complete')\
            .format('console')\
            .start()
        
        query.awaitTermination()
        query1.awaitTermination()
    except Exception as e:
        print(f"THIS IS THE ERROR: {e}")
        sys.exit(1)