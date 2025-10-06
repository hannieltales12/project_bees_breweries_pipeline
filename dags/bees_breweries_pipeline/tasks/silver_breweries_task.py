from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, initcap
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from bees_breweries_pipeline.tools.const.schema_tables import SchemaTables


class SilverBreweriesTask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.spark = (
            SparkSession.builder.appName("SilverBreweriesTreatment")
            .master("spark://spark-master:7077")
            # Adiciona os pacotes de dependência S3A
            .config(
                "spark.jars",
                "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.367.jar",
            )
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .getOrCreate()
        )

    def execute(self, context) -> None:
        """
        Method responsible for reading data from the Bronze layer
        process the data and writing it to the Silvesr layey
        """

        dag_id = context["task"].dag_id

        self.log.info(f"Starting Silver treatment for dag: {dag_id}")

        input_dir = "s3a://bees-breweries/bronze/breweries/"
        output_path = "s3a://bees-breweries/silver/breweries/"

        df = self._read_bronze(input_dir)

        df_treated = self._treatment_df(df)

        df_partitioned = self._partition_df(df_treated)

        self._save_data(df_partitioned, output_path)

    def _read_bronze(self, input_dir: str) -> DataFrame:
        """
        Read data in parquet at bronze layer

        Args:
            input_dir (str): bronze dir

        Returns:
            df: dataframe with the data
        """

        try:

            self.log.info(f"Reading parquet from {input_dir}")

            df = self.spark.read.parquet(input_dir)

            return df

        except Exception as e:
            raise Exception(f"Error read data: {e}")

    def _treatment_df(self, df: DataFrame) -> DataFrame:
        """
        Treatment data in the Silver layer

        Args:
            df (Dataframe): Dataframe with the data

        Returns:
            df_treated: dataframe with the data treated
        """

        self.log.info("Starting Silver treatment")

        schema_breweries = SchemaTables.BREWERIES_SCHEMA

        # add schema
        try:
            df_breweries = df.select(
                [
                    col(column).alias(schema_breweries[column])
                    for column in schema_breweries
                ]
            )

        except Exception as err:
            raise Exception(f"Fail to add schema: {err}")

        # remove duplicates
        try:
            df_breweries = df_breweries.dropDuplicates()

        except Exception as err:
            raise Exception(f"Fail to remove duplicates: {err}")

        # treatment null values
        try:
            string_cols = [
                f.name
                for f in df_breweries.schema.fields
                if f.dataType.simpleString() == "string"
            ]

            df_breweries = df_breweries.na.fill("", subset=string_cols)

            df_breweries = df_breweries.na.fill(
                0.0, subset=["longitude", "latitude"]
            )

        except Exception as err:
            raise Exception(f"Fail to remove null values: {err}")

        # tretment values

        try:

            df_breweries = df_breweries.withColumn(
                "country", trim(col("country"))
            )

            df_breweries = df_breweries.withColumn(
                "country", initcap(trim(col("country")))
            )

        except Exception as err:
            raise Exception(f"Fail to trim values: {err}")

        self.log.info("Silver treatment completed successfully")
        return df_breweries

    def _partition_df(self, df: DataFrame) -> DataFrame:
        """
        Partition data in the Silver layer

        Args:
            df (Dataframe): Dataframe with the data

        Returns:
            df_partitioned: dataframe with the data treated
        """

        self.log.info("Starting Silver partition")

        # Partition to country
        df_partitioned = df.withColumn("country_value", col("country"))

        self.log.info("Silver partition completed successfully")

        return df_partitioned

    def _save_data(self, dataframe: DataFrame, output_path: str) -> None:
        """
        Saves data in the Silver layer

        Args:
            dataframe (Dataframe): Dataframe with the data
            output_path (str): Silver dir
        """
        try:

            self.log.info(f"Writing Parquet to {output_path}")

            dataframe.write.partitionBy("country_value").mode(
                "overwrite"
            ).parquet(output_path)

            self.log.info("Silver transformation completed successfully.")

            self.spark.stop()

        except Exception as e:
            raise Exception(f"Error saving data: {e}")
