"""Select the feature columns for modeling"""
import os

import luigi
import pandas as pd

import definitions
from src.download.download_data import DownloadData


class SelectFeatures(luigi.Task):

    FEATURE_COLUMN_PREFIX = "feature"

    def requires(self):
        """Requires that the data is first downloaded"""
        return DownloadData()

    def output(self):
        """Where is the output file saved"""
        tournament_data_name = self.requires().get_current_tournament_file_name()
        return luigi.LocalTarget(
            os.path.join(
                definitions.DATA_DIR,
                "features",
                tournament_data_name + ".csv"
            )
        )

    def select_feature_columns(self, tournament_data):
        return [
            f for f in tournament_data.columns if f.startswith(self.FEATURE_COLUMN_PREFIX)
        ]

    def run(self):
        """Select the feature columns for modeling"""
        tournament_data = pd.read_csv(self.requires().output().path)

        print(tournament_data.head())
        feature_columns = self.select_feature_columns(tournament_data)

        feature_data = tournament_data.loc[:, feature_columns]
        print(feature_data)
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)

        feature_data.to_csv(self.output().path)

        return feature_data


if __name__ == "__main__":
    download_data_task = SelectFeatures()
    luigi.build(
        [
            download_data_task
        ],
        local_scheduler=True
    )
