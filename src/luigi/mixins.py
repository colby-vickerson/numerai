"""Luigi base classes to inherit from"""
import luigi
import src.utils.generic_utils as gen_utils


class TaskMixin(luigi.Task):
    """Task that contains generic helper methods used by child classes"""

    def load_output(self):
        """Load and return the task output"""
        return gen_utils.load_data_as_pandas(
            self.output().path
        )

    def save_data(self, data_to_save):
        """Save the data to the task output"""
        gen_utils.save_pandas_dataframe(data_to_save, self.output().path)
