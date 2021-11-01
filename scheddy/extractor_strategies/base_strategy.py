
from abc import ABCMeta, abstractmethod


class Strategy(metaclass=ABCMeta):
    def __init__(self):
        self._name = "base_strategy"

    def assign_files_to_extractor(self, files):
        pass
