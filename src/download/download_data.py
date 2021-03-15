"""Download the latest data from Numerai AI"""
import os
import luigi
import numerapi
import definitions


class DownloadData(luigi.Task):
    """Install data from Numerai"""

    output_directory = luigi.Parameter()

    def run(self):
        napi = numerapi.NumerAPI(verbosity="info")

        # download data
        napi.download_current_dataset(dest_path=self.output().path, unzip=True)

    def output(self):
        output_path = os.path.join(
            definitions.RAW_DATA_DIR,
            f"{definitions.NUMERAI_MODELING_DATA}.parquet.gzip"
        )
        return luigi.LocalTarget(
            output_path
        )
