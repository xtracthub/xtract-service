
from scheduler.extractor_strategies.extractor_strategy import Strategy


class ExtensionMapStrategy(Strategy):

    def __init__(self):
        super().__init__()

    def assign_files_to_extractor(self, files):
        """ The function that maps a list of files (many) to a probability vector of extractors (many) """
        for file in files:
            print(file)


strat = ExtensionMapStrategy()
strat.assign_files_to_extractor(files=['banana', 'pear'])

