#
# OtterTune - ESGYNDB.py
#
# Copyright (c) 2017-18, Carnegie Mellon University Database Group
#
'''
Created on July 31, 2019

@author: dvanaken
'''

import re

from .base import BaseParser
from website.models import DBMSCatalog
from website.types import DBMSType, KnobUnitType
from website.utils import ConversionUtil


class ESGYNDBParser(BaseParser):

    def __init__(self, dbms_id):
        super(ESGYNDBParser, self).__init__(dbms_id)
        self.valid_true_val = ["on", "true", "yes", "1","1.0"]
        self.valid_false_val = ["off", "false", "no", "0","0.0"]

    ESGYNDB_BYTES_SYSTEM = [
        (1024 ** 5, 'PB'),
        (1024 ** 4, 'TB'),
        (1024 ** 3, 'GB'),
        (1024 ** 2, 'MB'),
        (1024 ** 1, 'kB'),
        (1024 ** 0, 'B'),
    ]

    ESGYNDB_TIME_SYSTEM = [
        (1000 * 60 * 60 * 24, 'd'),
        (1000 * 60 * 60, 'h'),
        (1000 * 60, 'min'),
        (1000, 's'),
        (1, 'ms'),
    ]

    ESGYNDB_BASE_KNOBS = {
    }

    @property
    def base_configuration_settings(self):
        return dict(self.ESGYNDB_BASE_KNOBS)

    @property
    def knob_configuration_filename(self):
        return 'ESGYNDB.conf'

    @property
    def transactions_counter(self):
        return 'global.throughput'

    @property
    def latency_timer(self):
        return 'global.latency'

    def convert_integer(self, int_value, metadata):
        converted = None
        try:
            converted = super(ESGYNDBParser, self).convert_integer(
                int_value, metadata)
        except ValueError:
            if metadata.unit == KnobUnitType.BYTES:
                converted = ConversionUtil.get_raw_size(
                    int_value, system=self.ESGYNDB_BYTES_SYSTEM)
            elif metadata.unit == KnobUnitType.MILLISECONDS:
                converted = ConversionUtil.get_raw_size(
                    int_value, system=self.ESGYNDB_TIME_SYSTEM)
            else:
                raise Exception(
                    'Unknown unit type: {}'.format(metadata.unit))
        if converted is None:
            raise Exception('Invalid integer format for {}: {}'.format(
                metadata.name, int_value))
        return converted

    def format_integer(self, int_value, metadata):
        int_value = int(round(int_value))
        if metadata.unit != KnobUnitType.OTHER and int_value > 0:
            if metadata.unit == KnobUnitType.BYTES:
                int_value = ConversionUtil.get_human_readable(
                    int_value, ESGYNDBParser.ESGYNDB_BYTES_SYSTEM)
            elif metadata.unit == KnobUnitType.MILLISECONDS:
                int_value = ConversionUtil.get_human_readable(
                    int_value, ESGYNDBParser.ESGYNDB_TIME_SYSTEM)
            else:
                raise Exception(
                    'Invalid unit type for {}: {}'.format(
                        metadata.name, metadata.unit))
        else:
            int_value = super(ESGYNDBParser, self).format_integer(
                int_value, metadata)
        return int_value

    def parse_version_string(self, version_string):
        dbms_version = version_string.split(',')[0]
        return re.search(r'\d+\.\d+(?=\.\d+)', dbms_version).group(0)


class ESGYNDB26Parser(ESGYNDBParser):

    def __init__(self, version):
        dbms = DBMSCatalog.objects.get(
            type=DBMSType.ESGYNDB, version='2.6.2')
        super(ESGYNDB26Parser, self).__init__(dbms.pk)
