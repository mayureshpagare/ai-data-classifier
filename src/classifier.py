# src/classifier.py
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, array_contains, lit
from pyspark.sql.types import StringType, ArrayType
import re
import os # For checking environment variables for Spark master

# Import keywords from our config file
from config import CLASSIFICATION_KEYWORDS

def classify_complaint(text):
    """
    Classifies a customer complaint based on predefined keywords.
    Returns a list of matching categories.
    """
    if text is None:
        return []

    text_lower = text.lower()
    matched_categories = []
    for category, keywords in CLASSIFICATION_KEYWORDS.items():
        for keyword in keywords:
            if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                matched_categories.append(category)
                break # Move to the next category once a keyword is found for current category
    return list(set(matched_categories)) # Return unique categories

# Register the UDF for Spark
# We are returning an ArrayType of StringType for the categories
classify_complaint_udf = udf(classify_complaint, ArrayType(StringType()))

def run_classifier():
    """
    Initializes Spark Session, loads data, classifies it, and displays results.
    """
    # Initialize Spark Session
    # Using 'local[*]' for local development, it will use all available cores.
    # In a real cluster, you would configure the master URL (e.g., 'spark://<master-ip>:7077')
    # For Docker, we'll run in local mode unless explicitly connected to a Spark cluster.
    spark = SparkSession.builder \
        .appName("AI Enhanced Data Classifier") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    print("Spark Session created successfully!")

    # --- Simulate Data Loading ---
    # In a real scenario, you would read from a source like:
    # df = spark.read.json("s3://your-bucket/customer_complaints.json")
    # df = spark.read.csv("hdfs://your-path/complaints.csv", header=True)
    # For now, we'll create a sample DataFrame.

    sample_data = [
        (1, "My internet connection is really slow and keeps dropping."),
        (2, "I have an incorrect charge on my last bill. Please check."),
        (3, "The application crashed unexpectedly, can you fix this bug?"),
        (4, "I'd like to suggest a new feature for the reporting dashboard."),
        (5, "I can't log in, it says error 404."),
        (6, "What is the procedure for account activation?"),
        (7, "The customer service was really bad, totally unresponsive."),
        (8, "My payment didn't go through, and I got an invoice reminder."),
        (9, "I want to complain about the constant errors in the software."),
        (10, None) # Example with a null complaint
    ]

    # Create a Spark DataFrame
    schema = ["id", "complaint_text"]
    df = spark.createDataFrame(sample_data, schema=schema)

    print("\n--- Original Data ---")
    df.show(truncate=False)

    # --- Apply Classification ---
    classified_df = df.withColumn(
        "classified_categories",
        classify_complaint_udf(col("complaint_text"))
    )

    print("\n--- Classified Data ---")
    classified_df.show(truncate=False)

    # --- Further Processing/Analysis (Optional) ---
    # Example: Filter complaints related to 'Technical Issue'
    tech_issues_df = classified_df.filter(array_contains(col("classified_categories"), "Technical Issue"))
    print("\n--- Technical Issues Complaints ---")
    tech_issues_df.show(truncate=False)

    # Example: Count complaints per category (simple approach for multiple categories)
    # This is more complex if a complaint can belong to multiple categories.
    # For a simple count, you might explode the categories or count occurrences.
    # For demonstration, let's count complaints that *contain* a certain category
    print("\n--- Category Counts (for demonstration) ---")
    for category in CLASSIFICATION_KEYWORDS.keys():
        count = classified_df.filter(array_contains(col("classified_categories"), category)).count()
        print(f"Number of '{category}' complaints: {count}")

    # --- SQL Integration Example ---
    # Register the classified DataFrame as a temporary view
    classified_df.createOrReplaceTempView("customer_complaints_classified")

    print("\n--- SQL Query Example: Complaints with Billing Inquiry ---")
    spark.sql("""
        SELECT id, complaint_text, classified_categories
        FROM customer_complaints_classified
        WHERE array_contains(classified_categories, 'Billing Inquiry')
    """).show(truncate=False)

    # --- Output/Sink Classified Data ---
    # In a real pipeline, you would write this to a persistent store:
    # classified_df.write.mode("overwrite").parquet("s3://your-output-bucket/classified_complaints.parquet")
    # classified_df.write.mode("overwrite").jdbc(url=jdbcUrl, table="classified_data", properties=connectionProperties)
    print("\n--- Data Classification Complete ---")

    spark.stop()
    print("Spark Session stopped.")

if __name__ == "__main__":
    run_classifier()