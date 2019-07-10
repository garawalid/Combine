class Pipeline:

    def __init__(self, id, name, data_source, data_destination, responsible, table_name=None):
        # Abstract class of Pipeline
        self.id = id
        self.name = name
        self.data_source = data_source
        self.data_destination = data_destination
        self.responsible = responsible
        if table_name is None:
            raise ValueError('We cannot initiate the pipeline. Please pass the datalake_path.')
        else:
            self.table_name = table_name

    def run(self):
        # launch pipeline
        pass

    def _preprocess(self):
        # check the quality of the data
        # Compute stats
        pass

    def _connect(self):
        # Fetch data from the source
        pass

    def _process(self):
        # Import transformations either from batch or streaming layer
        pass

    def _postprocess(self):
        # Collect metadata about the pipeline
        pass

    def _save(self):
        # save output into a datawarehouse
        pass
