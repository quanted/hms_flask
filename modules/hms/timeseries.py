

class TimeSeriesOutput:

    def __init__(self, source: str, dataset: str):
        self.datasource = source
        self.dataset = dataset
        self.metadata = {}
        self.data = {}

    def add_metadata(self, key: str, value: str, i: int = 0):
        if key not in self.metadata.keys():
            self.metadata[key] = str(value)
        else:
            i += 1
            key_i = f"{key}-{i}"
            if key_i in self.metadata.keys():
                self.add_metadata(key, value, i)
            else:
                self.metadata[key_i] = str(value)

    def add_timestep(self, dt: str, values: list):
        if dt not in self.data.keys():
            self.data[dt] = values

    def to_dict(self):
        return {
            "datasource": self.datasource,
            "dataset": self.dataset,
            "metadata": self.metadata,
            "data": self.data
        }
