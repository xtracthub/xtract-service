
from scheddy.extractor_strategies.base_strategy import Strategy
from scheddy.maps.extension_map import INVERTED_EXTENSION_MAP


class ExtensionMapStrategy(Strategy):

    def __init__(self):
        super().__init__()

    def assign_files_to_extractors(self, file_objs):

        """ The function that maps a list of files (many) to a probability vector of extractors (many).
            Note: these should be json.loads'ed prior to arrival here!

            :returns listof(tuples) --> list of (priority, extractor) pairs to be used in downstream processing.
            The priority for ALL extension_map extractors is 1 if extractor as there is no ranking involved.
        """

        print(f"Len: {type(file_objs[0])}")
        assert len(file_objs) == 1, "[ExtensionMapStrategy] Number of families sent here should be 1"
        assert len(file_objs[0]['files']) == 1, "[ExtensionMapStrategy] Number of files per family should be 1"
        file_obj = file_objs[0]

        if file_obj['files'][0]['metadata']['physical']['extension'] is not None:
            extension = file_obj['files'][0]['metadata']['physical']['extension'].lower()
        else:
            extension = "N/A"

        # IF we even have a mappable extension, THEN find corresponding extractor.
        if extension in INVERTED_EXTENSION_MAP:
            extractor = INVERTED_EXTENSION_MAP[extension]
        else:
            extractor = "unknown"

        # This is only here for clarify that one file can map to many extractors in probabilistic model.
        extractors = [(1, extractor)]

        return extractors
