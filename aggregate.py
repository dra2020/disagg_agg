# Script to aggregate data from blocks to vtds
#
# 2019 

import json
from tqdm import tqdm
import pprint
import math

from . import agg_logging as log

def handle_field(dest_block_map, dest_key, ok_to_agg, result_props, props_total, props_count, key, value):
  if not (key in dest_block_map):
    tot = value["TOT"] if "TOT" in value else 0
    log.dprint("blk not in map: " + key + ", Tot: " + str(tot))
  else:
    vtd = dest_block_map[key]
    if not (vtd in result_props):
        result_props[vtd] = {dest_key: vtd}
    for prop, prop_value in value.items():
        if prop != dest_key and ok_to_agg(prop):
            try:
                if math.isnan(prop_value):
                    prop_value = 0
                if prop in props_total:
                    props_total[prop] += prop_value
                else:
                    props_total[prop] = prop_value

                if prop in props_count:
                    props_count[prop] += 1
                else:
                    props_count[prop] = 1

                if prop in result_props[vtd]:
                    result_props[vtd][prop] += prop_value
                else:
                    result_props[vtd][prop] = prop_value
            except:
              log.dprint("Non number prop: ", prop)


def make_aggregated_props(block_props_path, dest_block_map_path, dest_key, ok_to_agg, srcIsGeojson=False):
    """
        block_props: block fields to be aggregated {blkid: {field1: value1, ...}}  [if not SrcIsGeojson]
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
    if not srcIsGeojson:
        for key, value in tqdm(block_props.items()):
            handle_field(dest_block_map, dest_key, ok_to_agg, result_props, props_total, props_count, key, value)
    else:
        for feature in tqdm(block_props["features"]):
            props = feature["properties"]
            geoid = props["GEOID"]
            handle_field(dest_block_map, dest_key, ok_to_agg, result_props, props_total, props_count, geoid, props)


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
        
