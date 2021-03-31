"""Select the feature columns for modeling"""
import os

import luigi

import definitions
from src.download.download_data import DownloadTrainingData
from src.luigi.mixins import TaskMixin
from src.luigi.salt import salted_target


class SelectFeatures(TaskMixin):

    output_directory: str = luigi.Parameter(
        description="Absolute path to the output directory where the file is saved",
        default=definitions.RAW_DATA_DIR
    )

    FEATURE_COLUMN_PREFIX = "feature"

    def requires(self):
        """Requires that the data is first downloaded"""
        return DownloadTrainingData()

    def output(self):
        """Where is the output file saved"""
        tournament_data_name = self.requires().get_current_tournament_file_name()
        return salted_target(
            self,
            os.path.join(
                definitions.DATA_DIR,
                "features",
                tournament_data_name + "_{salt}.csv"
            )
        )

    def select_feature_columns(self, tournament_data):
        return [
            f for f in tournament_data.columns if f.startswith(self.FEATURE_COLUMN_PREFIX)
        ]

    def run(self):
        """Select the feature columns for modeling"""
        tournament_data = self.requires().load_output()

        feature_columns = self.select_feature_columns(tournament_data)

        feature_data = tournament_data.loc[:, feature_columns]

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)

        self.save_data(feature_data)

        return feature_data


if __name__ == "__main__":
    download_data_task = SelectFeatures()
    luigi.build(
        [
            download_data_task
        ],
        local_scheduler=True
    )
