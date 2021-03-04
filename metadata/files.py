
import os


class FileObj(): 

    def __init__(self, file_name, metadata, inflated=False):
        self.file_name = file_name
        self.file_metadata = metadata
        self.inflated = inflated
        self.extractors_tried = {"universal", "sampler"}
        
    def delete_from_local(self):
        os.remove(self.file_name)

    def create_gmeta(self):
        return "[TODO] return Globus .gmeta file"
