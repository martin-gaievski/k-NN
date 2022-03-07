# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""Base Parser class.

Classes:
    BaseParser: Base class for config parsers.

Exceptions:
    ConfigurationError: An error in the configuration syntax.
"""

import os
from io import TextIOWrapper

import cerberus

from okpt.io.utils import reader


class ConfigurationError(Exception):
    """Exception raised for errors in the tool configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str):
        self.message = f'{message}'
        super().__init__(self.message)


def _get_validator_from_schema_name(schema_name: str):
    """Get the corresponding Cerberus validator from a schema name."""
    curr_file_dir = os.path.dirname(os.path.abspath(__file__))
    schemas_dir = os.path.join(os.path.dirname(curr_file_dir), 'schemas')
    schema_file_path = os.path.join(schemas_dir, f'{schema_name}.yml')
    schema_obj = reader.parse_yaml_from_path(schema_file_path)
    return cerberus.Validator(schema_obj)


class BaseParser:
    """Base class for config parsers.

    Attributes:
        validator: Cerberus validator for a particular schema
        errors: Cerberus validation errors (if any are found during validation)

    Methods:
        parse: Parse config.
    """

    def __init__(self, schema_name: str):
        self.validator = _get_validator_from_schema_name(schema_name)
        self.errors = ''

    def parse(self, file_obj: TextIOWrapper):
        """Convert file object to dict, while validating against config schema."""
        config_obj = reader.parse_yaml(file_obj)
        is_config_valid = self.validator.validate(config_obj)
        if not is_config_valid:
            raise ConfigurationError(self.validator.errors)

        return self.validator.document
