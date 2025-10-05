import os
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
            .getOrCreate()
        )

    def execute(self, context) -> None:
        """
        Method responsible for reading data from the Landing layer and writing it to the Bronze layer in .parquet format
        """

        dag_id = context["task"].dag_id

        self.log.info(f"Starting RAW transformation for task: {dag_id}")

        input_dir = f"/opt/airflow/.storage/landing/{dag_id}/"
        output_dir = f"/opt/airflow/.storage/bronze/{dag_id}/"

        dataframe = self._read_landing(input_dir)

        self._save_data(dataframe, output_dir)

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

            df = self.spark.read.json(input_dir)

            df.printSchema()

            return df

        except Exception as e:
            raise Exception(f"Error read data: {e}")

    def _save_data(self, dataframe: DataFrame, output_dir: str) -> None:
        """
        Saves data in the Bronze layer

        Args:
            dataframe (Dataframe): Dataframe with the data
        """
        try:
            os.makedirs(output_dir, exist_ok=True)

            parquet_path = os.path.join(output_dir, "breweries.parquet")

            self.log.info(f"Writing Parquet to {parquet_path}")

            (
                dataframe.coalesce(1)
                .write.mode("overwrite")
                .parquet(parquet_path)
            )

            self.log.info("RAW transformation completed successfully.")

            self.spark.stop()

        except Exception as e:
            raise Exception(f"Error saving data: {e}")
