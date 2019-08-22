# Script to verify (a little) the result of disaggregating and aggregating
#
#
# 2019

import pandas as pd
from tqdm import tqdm

import geopandas as gpd
import pprint
import json
import math
import numbers

from . import agg_logging as log

def verify_source_vs_aggregated(source_data_path, agg_data_from_source_path, ok_to_agg, block_data_path=None):
    """
        Function totals each numeric field across source data rows, does the same across aggregated data rows,
        and then compares. The expectation is that the diff of each field between the original source (that got disaggregated) and resulting
        aggregated data in the destination geometry, should have ABS value less than 1.
        Results are printed to stdout.

        Optionally, block data can be given and that is compared against the source as well. 
    """

    # Source and aggregated can be geojson or shapefiles; we're looking only at the data 
    source_data = gpd.read_file(source_data_path)
    agg_data = gpd.read_file(agg_data_from_source_path)

    # Build map of total values for all number fields in source
    source_keys = source_data.keys()
    source_props_total = {}
    for i in tqdm(source_data.index):
        for key in source_keys:
            try:
                value = source_data.loc[i, key]
                if ok_to_agg(key):
                    num = float(value)
                    if key in source_props_total:
                        source_props_total[key] += num
                    else:
                        source_props_total[key] = num
            except:
                pass
            
    # Build map of total values for all number fields in agg
    agg_keys = agg_data.keys()
    agg_props_total = {}
    for i in tqdm(agg_data.index):
        for key in agg_keys:
            try:
                value = agg_data.loc[i, key]
                num = float(value)
                if math.isnan(num):
                    log.dprint("NaN: ", key)
                else:
                    if key in agg_props_total:
                        agg_props_total[key] += num
                    else:
                        agg_props_total[key] = num
            except:
                pass

    # Compare
    diff_props = {}
    for key, source_value in source_props_total.items():
        if key in agg_props_total:
            agg_value = agg_props_total[key]
            diff_props[key] = source_value - agg_value
    
    pp = log.pretty_printer()
    log.dprint("Source vs Aggregated")
    pp.pprint(diff_props)

    if block_data_path != None:
        # Block data is of the form {blkid: {field1 : value1, ...}}
        with open(block_data_path) as json_file:
            block_data = json.load(json_file)

        # Build map of total values for all number fields in agg
        block_props_total = {}
        for blkid, fields in tqdm(block_data.items()):
            for field, value in fields.items():
                try:
                    num = float(value)
                    if math.isnan(num):
                        log.dprint("NaN: ", key)
                    else:
                        if field in block_props_total:
                            block_props_total[field] += num
                        else:
                            block_props_total[field] = num
                except:
                    pass

        diff_props_block = {}
        for key, source_value in source_props_total.items():
            if key in block_props_total:
                block_value = block_props_total[key]
                diff_props_block[key] = source_value - block_value
        
        log.dprint("Source vs Block")
        pp.pprint(diff_props_block)
