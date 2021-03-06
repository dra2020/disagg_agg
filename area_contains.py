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

def make_target_source_allmap(source, target, source_key, target_key, use_index_for_source_key):
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
    
    print("Making map: Initialize source/target overlap map")
    for j in tqdm(target.index):
        contains_map[target.loc[j, target_key]] = []

    print("Making map: Intersect source and target geometries")
    count_in_possible_matches = 0
    for i in tqdm(source.index):
        source_row_shape = source.loc[i, 'geometry']
        source_row_key = i if use_index_for_source_key else str(source.loc[i, source_key])
        source_key_set.add(source_row_key)      
        possible_matches = [target.index[m] for m in list(si.intersection(source_row_shape.bounds))]
        
        if len(possible_matches) == 0:
            match = None
            
        elif len(possible_matches) == 1:
            match = possible_matches[0]
            contains_map[target.loc[match, target_key]].append((source_row_key, 1.0))
                
        else:
            for j in possible_matches:
                count_in_possible_matches += 1
                target_shape = target.loc[j, 'geometry']
                target_loc_key = target.loc[j, target_key]
                source_prec = None #source_prec = source.loc[source_row_key, 'PRECINCT']  # For matching precinct debugging
                try:
                    pct_in = source_row_shape.intersection(target_shape).area / target_shape.area
                    if pct_in > 0.001:
                        contains_map[target_loc_key].append((source_row_key, pct_in, source_prec))
                    elif pct_in > 0:
                        log.dprint("Very small in: source: ", source_row_key, ", target: ", target_loc_key)
                        contains_map[target_loc_key].append((source_row_key, pct_in, source_prec))
                    else:
                        contains_map[target_loc_key].append((source_row_key, -1, source_prec))
                except:
                    # do nothing
                    log.dprint("Likely intersection error: source key: ", source_row_key, ", target key: ", target_loc_key)
                    try:
                        if source_row_shape.contains(target_shape):
                            log.dprint("Contains")
                            contains_map[target_loc_key].append((source_row_key, 1.0, source_prec))
                        elif source_row_shape.overlaps(target_shape):
                            log.dprint("Overlaps")
                            contains_map[target_loc_key].append((source_row_key, 0.5, source_prec))
                        else:
                            contains_map[target_loc_key].append((source_row_key, -1, source_prec))
                    except:
                        log.dprint("Contains or Overlaps operation also failed")

    log.dprint("Possible match count: ", count_in_possible_matches)
    return contains_map, source_key_set

def make_target_source_map(larger_path, smaller_path, larger_key, smaller_key, use_index_for_larger_key, source_is_block_group=False):
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
    res_tuple = make_target_source_allmap(larger_shapes, smaller_shapes, larger_key, smaller_key, use_index_for_larger_key)

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

    log.dprint("Total blocks: ", total_blocks)
    log.dprint("Blocks not contained in anything: ", not_contained_blocks)
    log.dprint("Blocks contained based on BB: ", based_on_bb_blocks)
    log.dprint("Split blocks: ", split_count, "\n")

    #pp = log.pretty_printer()
    #pp.pprint(final_map)
   
    return final_map
