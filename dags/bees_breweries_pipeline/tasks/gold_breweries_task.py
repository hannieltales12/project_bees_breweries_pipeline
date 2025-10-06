from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoldBreweriesTask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.spark = (
            SparkSession.builder.appName("GoldBreweriesInject")
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
        Method responsible for reading data from the Silver layer
        create a business agregation and writing it to the Gold layey
        """

        dag_id = context["task"].dag_id

        self.log.info(f"Starting Gold inject business for dag: {dag_id}")

        input_dir = "s3a://bees-breweries/silver/breweries/"
        output_path = "s3a://bees-breweries/gold/breweries/"

        df = self._read_silver(input_dir)

        df_business = self._inject_business_df(df)

        df_partitioned = self._partition_df(df_business)

        self._save_data(df_partitioned, output_path)

    def _read_silver(self, input_dir: str) -> DataFrame:
        """
        Read data in the json format at bronze layer

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

    def _inject_business_df(self, df: DataFrame) -> DataFrame:
        """
        Inject a business agregation to the dataframe

        Args:
            df (Dataframe): Dataframe with the data

        Returns:
            df_treated: dataframe with the business injected
        """
        try:
            self.log.info("Starting gold business inject")

            df_gold = (
                df.groupBy("country", "state_province", "city", "brewery_type")
                .agg(count("*").alias("total_breweries"))
                .orderBy("country", "state_province", "city", "brewery_type")
            )
        except Exception as err:
            raise Exception(f"Fail to inject business: {err}")

        return df_gold

    def _partition_df(self, df: DataFrame) -> DataFrame:
        """
        Partition data in the Gold layer

        Args:
            df (Dataframe): Dataframe with the data

        Returns:
            df_partitioned: dataframe with the partition
        """

        try:
            self.log.info("Starting Gold partition")

            # Partition to country
            df_partitioned = df.withColumn("country_value", col("country"))

            self.log.info("Gold partition completed successfully")

            return df_partitioned

        except Exception as err:
            raise Exception(f"Fail to create partition: {err}")

    def _save_data(self, dataframe: DataFrame, output_path: str) -> None:
        """
        Saves data in the Gold layer

        Args:
            dataframe (Dataframe): Dataframe with the data
            output_path (str): Gold dir
        """
        try:

            self.log.info(f"Writing Parquet to {output_path}")

            # Partition to country
            dataframe.write.partitionBy("country_value").mode(
                "overwrite"
            ).parquet(output_path)

            self.log.info("Gold transformation completed successfully.")

            self.spark.stop()

        except Exception as e:
            raise Exception(f"Error saving data: {e}")
