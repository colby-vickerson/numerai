"""Download the latest data from Numerai AI"""
import os
import luigi
import numerapi
import definitions
from src.luigi.mixins import TaskMixin
from src.luigi.salt import salted_target


class DownloadTrainingData(TaskMixin):
    """Install data from Numerai"""

    output_directory: str = luigi.Parameter(
        description="Absolute path to the output directory where the file is saved",
        default=definitions.RAW_DATA_DIR
    )

    @staticmethod
    def get_numerapi_connection():
        return numerapi.NumerAPI(verbosity="info")

    def get_zipped_current_tournament_file_name(self):
        """Return the zipped file name of the tournament data"""
        return self.get_current_tournament_file_name() + ".zip"

    def get_current_tournament_file_name(self):
        """Get the latest tournament data file name"""
        napi = self.get_numerapi_connection()
        round_number = napi.get_current_round()
        return f"numerai_dataset_{round_number}"

    def remove_zipped_file(self):
        """Remove the zipped file from the download directory"""
        os.remove(
            os.path.join(
                self.output_directory,
                self.get_zipped_current_tournament_file_name()
            )
        )
        return 1

    def run(self):
        napi = self.get_numerapi_connection()

        # download data
        napi.download_current_dataset(
            dest_path=self.output_directory,
            unzip=True
        )

        # Remove the zipped file
        self.remove_zipped_file()

    def output(self):
        output_path = os.path.join(
            self.output_directory,
            self.get_current_tournament_file_name(),
            "numerai_training_data.csv"
        )
        return luigi.LocalTarget(output_path)


if __name__ == "__main__":
    download_data_task = DownloadTrainingData()
    luigi.build(
        [
            download_data_task
        ],
        local_scheduler=True
    )
