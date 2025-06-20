import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# Initialize job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read from RDS using JDBC connection with bookmarks
def get_jdbc_connection_options(table_name):
    jdbc_connection_options = {
        "url": "jdbc:mysql://your-rds-endpoint:3306/your_database",
        "dbtable": table_name,
        "user": "username", 
        "password": "password",
        "customJdbcDriverS3Path": "s3://bucket/mysql-connector-java.jar",
        "customJdbcDriverClassName": "com.mysql.jdbc.Driver"
    }
    return jdbc_connection_options

tables = ["table1", "table2", "table3"]  # List of tables to process
for table in tables:
    connection_options = get_jdbc_connection_options(table)
    # Read data from each table
    df_source = glueContext.create_dynamic_frame.from_options(
        connection_type="mysql",
        connection_options=connection_options,
        transformation_ctx=f"datasource_{table}"  # Unique transformation context for each table
    )
    
    # No transformation needed, just pass through the data
    df_transformed = df_source

    # Write to destination (e.g., S3)
    glueContext.write_dynamic_frame.from_options(
        frame=df_transformed,
        connection_type="s3",
        connection_options={"path": "s3://your-bucket/output/"},
        format="parquet",
        transformation_ctx=f"datasink_{table}"  # Unique transformation context for each table
    )


# connections_options = get_jdbc_connection_options("your_table_name")
# # Method 1: Using JDBC connection directly
# df_source = glueContext.create_dynamic_frame.from_options(
#     connection_type="mysql",
#     connection_options=connection_options,
#     transformation_ctx="datasource0"  # This is crucial for bookmarks
# )

# Method 2: Using Data Catalog (if your RDS table is cataloged)
# df_source = glueContext.create_dynamic_frame.from_catalog(
#     database="your_glue_database",
#     table_name="your_glue_table",
#     transformation_ctx="datasource0",
#     additional_options={"jobBookmarkKeys": ["id", "last_updated"]}
# )

# Your transformation code here
# ...

# # Write to destination
# glueContext.write_dynamic_frame.from_options(
#     frame=df_transformed,
#     connection_type="s3",
#     connection_options={"path": "s3://your-bucket/output/"},
#     format="parquet",
#     transformation_ctx="datasink0"  # Unique transformation context
# )

# Commit the job to update bookmarks
job.commit()
