"""Download the latest data from Numerai AI"""
import os
import luigi
import numerapi
import definitions


class DownloadData(luigi.Task):
    """Install data from Numerai"""

    output_directory: str = luigi.Parameter(
        description="Absolute path to the output directory where the file is saved",
        default=definitions.RAW_DATA_DIR
    )

    def get_numerapi_connection(self):
        return numerapi.NumerAPI(verbosity="info")

    def get_tournament_data_name(self):
        """Get the latest tournament data file name"""
        napi = self.get_numerapi_connection()
        round_number = napi.get_current_round()
        return f"numerai_dataset_{round_number}"

    def run(self):
        napi = self.get_numerapi_connection()

        # download data
        napi.download_current_dataset(dest_path=self.output_directory, unzip=True)

        # Write a success flag to the unzipped file
        with open(self.output().path, "w") as file:
            file.write(f"Downloaded data: {self.get_tournament_data_name()}")

    def output(self):
        output_path = os.path.join(
            self.output_directory,
            self.get_tournament_data_name(),
            "SUCCESS.txt"
        )
        return luigi.LocalTarget(
            output_path
        )
