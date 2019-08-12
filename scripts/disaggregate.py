# Script to disaggregate data
#
# 2019
#


import geopandas as gpd
import json
import csv
from tqdm import tqdm
import numbers

import aggregate
import agg_logging as log

def make_block_props_map(source_props_path, block_map_path, block_pop_map, source_key, use_index_for_source_key):
    """
        source_props is geojson or shapefile
        block_map {blkid: source_key, ...}
        source_key is key field for source_props; value used to lookup in block_map; we always treat it as a string
        block_pop_map {blkid: population, ...} 
        use_index_for_source_key: True ==> Use geopandas index as key
    """

    source_props = gpd.read_file(source_props_path)
    with open(block_map_path) as json_file:
        block_map = json.load(json_file)

    # build, for each source key, a list of blocks in that source
    # src_blk_map = {srckey : [(blkid, pop), ...], ...}
    log.dprint("Build source to block/pop map")
    src_blk_map = {}
    count_blocks_not_in_source = 0
    count_blocks_not_in_source_with_nonzero_pop = 0
    for i in source_props.index:
        src_blk_map[i if use_index_for_source_key else str(source_props.loc[i, source_key])] = (i, [])
    for key, value in block_map.items():
        # value should be a source_key
        if key in block_pop_map:
            if value == "":
                # Means block was not in any larger (source) geometry
                count_blocks_not_in_source += 1
                error_block_population = block_pop_map[key]
                if error_block_population > 0:
                    log.dprint("Block not in source: ", key, ", Pop: ", error_block_population)
                    count_blocks_not_in_source_with_nonzero_pop += 1
            else:
                source_item = src_blk_map[int(value) if use_index_for_source_key else value]
                source_item[1].append((key, block_pop_map[key]))
        else:
            log.dprint("Block not in block_pop_map: ", key)

    log.dprint("Number of blocks not in any source: ", count_blocks_not_in_source) 
    log.dprint("Number of blocks not in any source with population: ", count_blocks_not_in_source_with_nonzero_pop) 

    log.dprint("Build block to fields map (disaggregate)")
    print("Build block to fields map (disaggregate)")
    final_blk_map = {}    # {blkid: {prop1: val1, prop2: val2, ...}, ...}
    for src_value in tqdm(src_blk_map.values()):
        sum_blk_pop = 0
        for blk_tuple in src_value[1]:
            sum_blk_pop += blk_tuple[1]
        for blk_tuple in src_value[1]:
            blk_pct = 0 if (sum_blk_pop == 0) else (blk_tuple[1] / sum_blk_pop)
            one_blk = {}
            source_props_item = source_props.loc[src_value[0]]
            for prop_key, prop_value in source_props_item.items():
                if isinstance(prop_value, numbers.Number) and aggregate.ok_to_agg(prop_key):
                    try:
                        # add float/int props only
                        one_blk[prop_key] = round(float(prop_value) * blk_pct, 3)
                    except:
                        #ignore
                        if prop_key[0:1] == 'G':
                            log.dprint("Prop Error: ", prop_key, ", ", prop_value, ", source_key: ", source_props.loc[src_value[0], "" if use_index_for_source_key else source_key])
                            one_blk[prop_key] = 0
                        dummy = 0

            final_blk_map[blk_tuple[0]] = one_blk
    
    return final_blk_map
            