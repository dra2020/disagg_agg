# Script to aggregate data from blocks to vtds
#
# 2019 

import json
from tqdm import tqdm
import pprint
import math

from . import agg_logging as log


def make_aggregated_props(block_props_path, dest_block_map_path, dest_key, ok_to_agg):
    """
        block_props: block fields to be aggregated {blkid: {field1: value1, ...}}
        dest_block_map: map of block to containing larger geo (e.g. precinct) {blkid: dest_key}

        Return geojson with empty geometry and all of the aggregated properties {..., features: [{properties: {dest_key: id, field1: value1, ...}, ...]}
    """

    with open(block_props_path) as json_file:
        block_props = json.load(json_file)
    with open(dest_block_map_path) as json_file2:
        dest_block_map = json.load(json_file2)

    log.dprint("Build map {dest_key: {field1: value1, ...}")
    print("Build map {dest_key: {field1: value1, ...}")
    result_props = {}
    props_total = {}
    props_count = {}
    for key, value in tqdm(block_props.items()):
        vtd = dest_block_map[key]
        if not (vtd in result_props):
            result_props[vtd] = {dest_key: vtd}
        for prop, prop_value in value.items():
            if prop != dest_key and ok_to_agg(prop):
                try:
                    float_value = float(prop_value)
                    if math.isnan(float_value):
                        float_value = 0
                    if prop in props_total:
                        props_total[prop] += float_value
                    else:
                        props_total[prop] = float_value

                    if prop in props_count:
                        props_count[prop] += 1
                    else:
                        props_count[prop] = 1

                    if prop in result_props[vtd]:
                        result_props[vtd][prop] += float_value
                    else:
                        result_props[vtd][prop] = float_value
                except:
                  log.dprint("Non number prop: ", prop)

    pp = log.pretty_printer()
    log.dprint("Props totals")
    pp.pprint(props_total)        

    log.dprint("Props Count")
    pp.pprint(props_count)        

    print("Build geojson")
    features = []
    for vtd, props in tqdm(result_props.items()):
        features.append({'type': 'Feature', 'geometry': None, 'properties': props})

    return {'type': 'FeatureCollection', 'features': features}
        
