
# Build from: https://python-jsonschema.readthedocs.io/en/stable/
from jsonschema import validate
from metadata.schemas import *
import json
import os


class MetadataValidator:

    schema_mappings = {"bert": "bert.json",
                       "image": "image.json",
                       "tabular": "tabular.json",
                       "text_file": "text_file.json"}

    def validate(self, schema, mdata_obj, validator_type="file"):
        """Returns true if metadata adheres to schema.
           Else raises validation error.
           :param schema
           :param mdata_obj

           :raises ValidationError if mdata invalid
           :returns None
        """

        if os.path.isfile(f"schemas/{schema}.json"):
            print("inhere")

        # TODO: Check it

        # validate()

        # TODO: Require strict JSON.
        # TODO: Remove null/None values


mv = MetadataValidator()
mv.validate('bert', {})