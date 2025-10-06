from pyspark.sql import SparkSession, DataFrame
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BronzeBreweriesTask(BaseOperator):
    @apply_defaults
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.spark = (
            SparkSession.builder.appName("BronzeBreweriesTransformation")
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
        Method responsible for reading data from the Landing layer and writing it to the Bronze layer in .parquet format
        """

        dag_id = context["task"].dag_id

        self.log.info(f"Starting Bronze transformation for dag: {dag_id}")

        input_dir = f"/opt/airflow/.storage/landing/{dag_id}/"
        output_path = "s3a://bees-breweries/bronze/breweries/"

        dataframe = self._read_landing(input_dir)

        self._save_data(dataframe, output_path)

    def _read_landing(self, input_dir: str) -> DataFrame:
        """
        Read data in the json format at landing layer

        Args:
            input_dir (str): Landing dir

        Returns:
            df: dataframe with the data
        """

        try:

            self.log.info(f"Reading JSON from {input_dir}")

            df = self.spark.read.option("multiLine", True).json(input_dir)

            df.show()

            return df

        except Exception as e:
            raise Exception(f"Error read data: {e}")

    def _save_data(self, dataframe: DataFrame, output_path: str) -> None:
        """
        Saves data in the Bronze layer

        Args:
            dataframe (Dataframe): Dataframe with the data
            output_path (str): Bronze dir
        """
        try:

            self.log.info(f"Writing Parquet to {output_path}")

            dataframe.write.mode("overwrite").parquet(output_path)

            self.log.info("Bronze transformation completed successfully.")

            self.spark.stop()

        except Exception as e:
            raise Exception(f"Error saving data: {e}")
