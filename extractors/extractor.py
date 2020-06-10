
from abc import ABCMeta, abstractmethod


class Extractor(metaclass=ABCMeta):

    def __init__(self, extr_id, extr_name, func_id, store_type, store_url):
        self.extr_id = extr_id
        self.func_id = func_id
        self.extr_name = extr_name
        self.store_type = store_type
        self.store_url = store_url
        self.extr_func = None

    def register_image(self):
        pass

    def set_extr_func(self, func):
        self.extr_func = func
