  # Script adapted from areal_interpolations.py, courtesy of Princeton Gerrymandering Project
#
# 2019
#
import pandas as pd
from tqdm import tqdm

import geopandas as gpd
import pprint
import json

from . import agg_logging as log

# Special work for Florida
flcounty_map = {
'073': 'LEO', '035': 'FLA', '037': 'FRA', '033': 'ESC', '127': 'VOL', '101': 'PAS', '107': 'PUT', '131': 'WAL', 
'027': 'DES', '071': 'LEE', '041': 'GIL', '053': 'HER', '117': 'SEM', '125': 'UNI', '051': 'HEN', '015': 'CHA', '047': 'HAM', 
'077': 'LIB', '113': 'SAN', '129': 'WAK', '133': 'WAS', '111': 'STL', '021': 'CLL', '017': 'CIT', '013': 'CAL', '005': 'BAY', 
'065': 'JEF', '007': 'BRA', '059': 'HOL', '093': 'OKE', '109': 'STJ', '115': 'SAR', '023': 'CLM', '103': 'PIN', '057': 'HIL', 
'079': 'MAD', '055': 'HIG', '039': 'GAD', '097': 'OSC', '019': 'CLA', '081': 'MAN', '069': 'LAK', '105': 'POL', '009': 'BRE', 
'085': 'MRT', '003': 'BAK', '119': 'SUM', '083': 'MRN', '049': 'HAR', '063': 'JAC', '067': 'LAF', '087': 'MON', '086': 'DAD', 
'061': 'IND', '011': 'BRO', '099': 'PAL', '031': 'DUV', '001': 'ALA', '123': 'TAY', '089': 'NAS', '095': 'ORA', '045': 'GUL', 
'029': 'DIX', '075': 'LEV', '121': 'SUW', '091': 'OKA', '043': 'GLA'}

def cntyid(countyfp):
    return flcounty_map[countyfp]


def make_target_source_allmap(source, target, source_key, target_key, use_index_for_source_key, state, year, isDemographicData):
    """
    Function returns a map (dictionary) representing the mapping of target poly to source polys
    
    Arguments
    ---------
    source: GeoPandas GeoDataFrame
        consisting of larger polygons
    target: GeoPandas GeoDataFrame
        consisting of smaller polygons typically expected to be contained in larger ones

    Outputs
    -------
    dict with keys = target.<target_key> and values = [{source.<source_key>: <area %>}]
    """

    si = target.sindex
    contains_map = {}  # {<target_key> : [{<source_key> : <%area>},...]}
    source_key_set = set({})  # a set we can reference later

    #filteredsource = source[source['County'] == 'MON']
    #print(len(source.index))
    #print(len(filteredsource.index))

    print("Making map: Initialize source/target overlap map")
    for j in tqdm(target.index):
        contains_map[target.loc[j, target_key]] = []

    print("Making map: Intersect source and target geometries")
    count_in_possible_matches = 0
    source_keys_in_set = set({})
    count_intersect_plus_failures = 0   # count if intersection throws exception and contains or overlaps throws, too
    count_intersect_failures = 0
    for i in tqdm(source.index):
        source_row_shape = source.loc[i, 'geometry']
        source_row_key = i if use_index_for_source_key else str(source.loc[i, source_key])
        source_key_set.add(source_row_key)
        if not hasattr(source_row_shape, 'bounds'):
            continue

        possible_matches = [target.index[m] for m in list(si.intersection(source_row_shape.bounds))]
        
        if len(possible_matches) == 0:
            match = None
            
        elif len(possible_matches) == 1:
            match = possible_matches[0]
            contains_map[target.loc[match, target_key]].append((source_row_key, 1.0))
            source_keys_in_set.add(source_row_key)
                
        else:
            for j in possible_matches:
                count_in_possible_matches += 1
                target_shape = target.loc[j, 'geometry']
                target_loc_key = target.loc[j, target_key]
                source_prec = None #source_prec = source.loc[source_row_key, 'PRECINCT']  # For matching precinct debugging
                try:
                    pct_in = source_row_shape.intersection(target_shape).area / target_shape.area
                    source_keys_in_set.add(source_row_key)
                    if pct_in > 0.001:
                        contains_map[target_loc_key].append((source_row_key, pct_in, source_prec))
                    elif pct_in > 0:
                        #log.dprint("Very small in: source: ", source_row_key, ", target: ", target_loc_key)
                        contains_map[target_loc_key].append((source_row_key, pct_in, source_prec))
                    else:
                        contains_map[target_loc_key].append((source_row_key, -1, source_prec))
                except:
                    # do nothing
                    #log.dprint("Likely intersection error: source key: ", source_row_key, ", target key: ", target_loc_key)
                    count_intersect_failures += 1
                    try:
                        source_keys_in_set.add(source_row_key)
                        if source_row_shape.contains(target_shape):
                            #log.dprint("Contains")
                            contains_map[target_loc_key].append((source_row_key, 1.0, source_prec))
                        elif source_row_shape.overlaps(target_shape):
                            #log.dprint("Overlaps")
                            contains_map[target_loc_key].append((source_row_key, 0.5, source_prec))
                        else:
                            contains_map[target_loc_key].append((source_row_key, -1, source_prec))
                    except:
                        log.dprint("Contains or Overlaps failed after Intersection failed")
                        count_intersect_plus_failures += 1

    source_keys_not_in_map = set({})
    for targkey, value in contains_map.items():
        for tuple in value:
            srckey = tuple[0]
            if not (srckey in source_keys_in_set):
                log.dprint("No blocks maps to source: ", srckey)
                source_keys_not_in_map.add(srckey)
    log.dprint("Source keys not in map: ", len(source_keys_not_in_map))

    if False: #  state == "FL" and (not isDemographicData):
        print("Begin Second Pass")
        source_keys_assigned_pass_2 = 0
        countykey = 'County' if year == 2018 else 'county'    # A pain that fl_2016 and fl_2018 are different
        for i in tqdm(target.index):
            target_loc_key = target.loc[i, target_key]
            countyfp = target_loc_key[2:5]
            filteredsource = source[source[countykey] == cntyid(countyfp)]
            if len(contains_map[target_loc_key]) == 0:
                #log.dprint("Pass 2 block has no target: ", target_loc_key)
                #print("Pass 2 block has no target: " + target_loc_key)
                target_shape = target.loc[i, 'geometry']
                closest = filteredsource.distance(target_shape).sort_values().index[0]
                source_row_key = closest if use_index_for_source_key else str(source.loc[closest, source_key])
                #print ("SourceRowKey: " + str(source_row_key))
                contains_map[target_loc_key].append((int(source_row_key), 0.5, None))
                source_keys_assigned_pass_2 += 1
        log.dprint("Phase 2 sources assigned: ", source_keys_assigned_pass_2)

    log.dprint("Intersect threw except count: ", count_intersect_failures)
    log.dprint("Intersect and Contains/Overlaps both threw except count: ", count_intersect_plus_failures)
    log.dprint("Possible match count: ", count_in_possible_matches, "\n")
    return contains_map, source_key_set

def make_target_source_map(larger_path, smaller_path, larger_key, smaller_key, use_index_for_larger_key, state, year, isDemographicData, source_is_block_group=False):
    """
    Given two file paths to larger (precinct) and smaller (block) geometry, opens files and calls
        make_target_source_allmap to produce tuple (contains_map, source_key_set)  (geometry files can be geojson or shapefile)
    larger_key is a string key uniquely identifying the rows in larger_path
    smaller_key is a string key uniquely identifying the rows in smaller_path

    Then walk thru contains_map (whose values are arrays of {<smaller_key>: %overlap}), building final map of smaller_key: larger_key.

    Note: if a block overlaps more than 1 precinct, it is assigned to the one with the greatest precentage overlap
    """
    #load in shapefiles 
    larger_shapes = gpd.read_file(larger_path)
    smaller_shapes = gpd.read_file(smaller_path)

    if larger_shapes.crs and smaller_shapes.crs: 
        print("Larger CRS: ", larger_shapes.crs)
        log.dprint("Larger CRS: ", larger_shapes.crs)
        print("Smaller CRS: ", smaller_shapes.crs)
        log.dprint("Smaller CRS: ", smaller_shapes.crs)
        print("Converting Larger CRS to Smaller")
        larger_shapes = larger_shapes.to_crs(smaller_shapes.crs)
    else:
        if not (larger_shapes.crs):
            print("Larger CRS unknown")
            log.dprint("Larger CRS unknown")
        if not (smaller_shapes.crs):
            print("Smaller CRS unknown")
            log.dprint("Smaller CRS unknown")
    res_tuple = make_target_source_allmap(larger_shapes, smaller_shapes, larger_key, smaller_key, use_index_for_larger_key, state, year, isDemographicData)

    print("Build target ==> source map")
    final_map = {}      # {res.key: <largest of res.value>}
    split_count = 0
    total_blocks = 0
    not_contained_blocks = 0
    based_on_bb_blocks = 0
    for key, value in res_tuple[0].items():
        total_blocks += 1
        if len(value) > 1:
            split_count += 1
            best_item = ('dummy', -2)
            for item in value:
                if item[1] > best_item[1]:
                    best_item = item
            if best_item[1] == -1:
                based_on_bb_blocks += 1
            final_map[key] = best_item[0]     # use [2] for precinct debugging
        elif len(value) == 0:
            if source_is_block_group:
                # block not found in anything; for larger geo = block_group, assign block to block_group by id
                bg_key = key[0:12]
                if bg_key in res_tuple[1]:
                    log.dprint("Block not contained in anything; using BG key: ", key)
                    final_map[key] = bg_key
                else:
                    log.dprint("Block not contained in anything: ", key)
                    final_map[key] = ''  # smaller not found in larger
                    not_contained_blocks += 1
            else:
                log.dprint("Block not contained in anything: ", key)
                final_map[key] = ''  # smaller not found in larger
                not_contained_blocks += 1

        else:
            if value[0][1] == -1:
                based_on_bb_blocks += 1
            final_map[key] = value[0][0]      # use [2] for precinct debugging

    source_key_set = res_tuple[1]
    source_keys_in_map = set({})
    for targkey, srckey in final_map.items():
        source_keys_in_map.add(srckey)
    log.dprint("Source keys not in map: ", len(source_key_set) - len(source_keys_in_map))
    print("Source keys not in map: " + str(len(source_key_set) - len(source_keys_in_map)))

    log.dprint("Total blocks: ", total_blocks)
    log.dprint("Blocks not contained in anything: ", not_contained_blocks)
    log.dprint("Blocks contained based on BB: ", based_on_bb_blocks)
    log.dprint("Split blocks: ", split_count, "\n")

    #pp = log.pretty_printer()
    #pp.pprint(final_map)
   
    return final_map
