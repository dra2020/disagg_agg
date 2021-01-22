# This script drives the steps to disaggregate and aggregate data for a state
#
# 2019

from tqdm import tqdm
import geopandas as gpd
import pprint
import json
import os
import csv

# *** Dependent modules that are part of the same repository
from . import statecodes                   # Maps two-letter state codes to two-digit Census state codes
from . import area_contains as ac          # Given 2 geometries produces map (dictionary) from smaller to larger
from . import disaggregate as disagg
from . import aggregate as agg
from . import disagg_agg_verify as verify
from . import agg_logging as log

# *** Dependent module that must be supplied by the user
#   prepare.prepare(state) is a hook to do any file moving/copying/unzipping/preprocessing
#   prepare.get_keys(state) returns a tuple of keys the three geometries (source_key, dest_key, block_key) (ex. GEOID10)
#   prepare.get_paths(state) returns a map (dictionary) of paths to all of the input and output files
#
#from . import prepare_disagg_agg_example as prepare
from .temp import prepare_disagg_agg as prepare

def ok_to_agg(prop):
    """
    Filter out props that we know we shouldn't aggregate
    """
    return (prop.lower()[0:4] != 'name' and prop.lower()[0:6] != 'county' and prop.lower()[0:8] != 'precinct' and prop.lower() != 'id' and
            prop.lower() != 'objectid' and prop.lower()[0:4] != 'area' and prop.lower() != 'pct' and prop.lower() != 'district' and
            prop.lower()[0:4] != 'fips' and prop.lower()[0:3] != 'cty' and prop.lower()[0:4] != 'ward' and prop.lower()[0:5] != 'geoid' and
            prop.lower() != 'blkgrp' and prop.lower()[0:6] != 'logrec' and prop.lower() != 'state' and prop.lower() != 'sumlevel' and
            prop.lower() != 'tract' and prop.lower()[0:7] != 'correct' and prop.lower() != 'vtd_name')


def make_block_map(state, stateCode, large_geo_path, large_geo_key, block_geo_path, block_key, block2geo_path, use_index_for_large_key=False, sourceIsBlkGrp=False):
    """
    Invokes area_contains: takes larger (precinct) geometry, smaller (block) geometry, and produces smaller ==> larger mapping (JSON)
    """
    if os.path.exists(block2geo_path):
        log.dprint("Block map already exists: ", block2geo_path)
        return

    log.dprint('Making block map:\n\t(', large_geo_path, ',', block_geo_path, ') ==>\n\t\t', block2geo_path) 
    block_map = ac.make_target_source_map(large_geo_path, block_geo_path, large_geo_key, block_key, use_index_for_large_key, sourceIsBlkGrp)

    log.dprint('Writing block map\n')
    with open(block2geo_path, 'w') as outf:
        json.dump(block_map, outf, ensure_ascii=False)


def disaggregate_data(state, stateCode, large_data_path, large_key, block2geo_path, block_key, block_pop_path, block_data_from_geo_path, use_index_for_large_key=False, isDemographicData=False):
    """
    Invokes disaggregate: takes larger (precinct) data, block population map, smaller-larger mapping, and produces smaller (block) data (JSON)
    """
    log.dprint('Making block_data_from_geo:\n\t(', large_data_path, ',', block_pop_path, ',', block2geo_path, ') ==>\n\t\t', block_data_from_geo_path)

    with open(block_pop_path) as block_pop_json:
        block_pop_map = json.load(block_pop_json)

    # Option here to supply different disaggregation algorithm for isDemographicData == True
    final_blk_map = disagg.make_block_props_map(log, large_data_path, block2geo_path, block_pop_map, large_key, use_index_for_large_key, ok_to_agg)

    log.dprint('Writing block_data_from_geo\n')
    with open(block_data_from_geo_path, 'w') as outf:
        json.dump(final_blk_map, outf, ensure_ascii=False)


def disaggregate_data_ca(state, stateCode, large_data_path, block2geo_path, block_key, block_data_from_geo_path):
    """
    Invokes disaggregate: takes larger (precinct) data, block population map, smaller-larger mapping, and produces smaller (block) data (JSON)
    """
    log.dprint('Making block_data_from_geo:\n\t(', large_data_path, ',', block2geo_path, ') ==>\n\t\t', block_data_from_geo_path)

    final_blk_map = disagg.make_block_props_map_ca(log, large_data_path, block2geo_path)

    log.dprint('Writing block_data_from_geo\n')
    with open(block_data_from_geo_path, 'w') as outf:
        json.dump(final_blk_map, outf, ensure_ascii=False)


def aggregate_source2dest(state, stateCode, block_data_path, block2geo_path, large_geo_key, dest_data_path):
    """
    Invokes aggregate: takes smaller (block) data, smaller-larger mapping, and produces larger (precinct) data (GEOJSON)
    """
    log.dprint ('Making dest_data:\n\t(', block_data_path, ',', block2geo_path, ') ==>\n\t\t', dest_data_path)
    aggregated_props = agg.make_aggregated_props(block_data_path, block2geo_path, large_geo_key, ok_to_agg)
    log.dprint('Writing dest data\n')
    with open(dest_data_path, 'w') as outf:
        json.dump(aggregated_props, outf, ensure_ascii=False)


def process_state(state, steps, sourceIsBlkGrp=False, isDemographicData=False, year=2016, isCVAP=False, destyear=2010): 
    """
    This function drives the steps in the disaggregation/aggregation process.
    Each step reads from its input files and writes to its output file, so steps can be taken one at a time if desired

    Input files (supplied by prepare module):
    -- source_geo_path (ex. 2016 precincts in order to map data from here) 
    -- source_data_path (either same as source geo file or keyed to that source geo) 
    -- dest_geo_path (ex. 2010 precincts in order to map data to here) 
    -- dest_data_path (either same as dest geo file or keyed to that dest geo; only if needed to disaggregate to blocks) 
    -- block_geo_path (ex. tl_2010_<stateCode>_tabblock10.shp) 

    Other inputs (supplied by prepare module):
    -- source_key: source geo/data row key (ex. "GEOID")
    -- dest_key: destination geo/data row key
    -- block_key: block geo row key
    -- use_index_for_source_key: True ==> use the geopanda row index instead of source_key; this can only be done is source_geo_path and source_data_path are identical

    Other inputs:
    -- state: 2-letter state code
    -- steps: tuple of steps to perform (can be any number of steps)
        -- 1: make block ==> source_geo mapping (to block2source_map_path)
              [Requires: source_geo_path, block_geo_path]
        -- 2: make block ==> dest_geo mapping (to block2dest_map_path)
              [Requires: dest_geo_path, block_geo_path]
        -- 3: disaggregate (to block_data_from_source_path) 
              [Requires: source_data_path, block2source_map_path, block_pop_path]
        -- 4: aggregate (to agg_data_from_source_path) 
              [Requires: block_data_from_source_path, block2dest_map_path]
        -- 5: disaggregate (to block_data_from_dest_path)           [Use this to get block data from the dest_geo/data]
              [Requires: dest_data_path, block2dest_map_path, block_pop_path]
        -- 6: verify 
              [Requires: source_data_path, agg_data_from_source_path]
    -- sourceIsBlkGrp: True ==> source_geo is block group geometry (allowing us to use that if blocks don't fall in any block group)
    -- isDemographicData: True ==> could use specific demographic population values to disaggregate, if available

    Produces files (paths must be specified by prepare module):
    -- block2source_map_path (ex: block_to_<sourceid>_map_<stateCode>.json)
    -- block2dest_map_path (ex: block_to_<destid>_map_<stateCode>.json)
    -- block_data_from_source_path (ex: block_from_<sourceid>_<stateCode>.json)
    -- block_data_from_dest_path (ex: block_from_<destid>_<stateCode>.json)
    -- agg_data_from_source_path (ex: <destid>_from_<sourceid>_<stateCode>.json)
    """

    stateCode = statecodes.make_state_codes()[state]      #  2-digit state census code
    source_key, dest_key, block_key, use_index_for_source_key = prepare.get_keys(state, not isDemographicData, year, destyear)

    paths = prepare.get_paths(state, not isDemographicData, year, isCVAP, destyear)
    source_geo_path = paths["source_geo_path"]
    source_data_path = paths["source_data_path"]
    block_geo_path = paths["block_geo_path"]
    dest_geo_path = paths["dest_geo_path"]
    dest_data_path = paths["dest_data_path"]
    block_pop_path = paths["block_pop_path"]
    block2source_map_path = paths["block2source_map_path"]
    block2dest_map_path = paths["block2dest_map_path"]
    block_data_from_source_path = paths["block_data_from_source_path"]
    block_data_from_dest_path = paths["block_data_from_dest_path"]
    agg_data_from_source_path = paths["agg_data_from_source_path"]
    working_path = paths["working_path"]

    logfile_name = "output" + "_".join(str(step) for step in steps) + "_" + str(year) + ".log"
    print("Setting logging output to ", working_path + logfile_name)
    log.set_output(working_path + logfile_name, "Disagg/Agg Logging")
 
    for step in steps:

        if (step == 1):
            log.dprint("*******************************************")
            log.dprint("****** 1: Make map between geometries *****")
            if (source_geo_path != None and block_geo_path != None and block2source_map_path != None):
                make_block_map(state, stateCode, source_geo_path, source_key, block_geo_path, block_key, block2source_map_path, use_index_for_source_key, sourceIsBlkGrp)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tSource geo: ", source_geo_path)
                log.dprint("\tBlock geo: ", block_geo_path)
                log.dprint("\tOutput path: ", block2source_map_path)

        elif (step == 2):
            log.dprint("*******************************************")
            log.dprint("****** 2: Make map between geometries *****")
            if (dest_geo_path != None and block_geo_path != None and block2dest_map_path != None):
                make_block_map(state, stateCode, dest_geo_path, dest_key, block_geo_path, block_key, block2dest_map_path)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tDest geo: ", dest_geo_path)
                log.dprint("\tBlock geo: ", block_geo_path)
                log.dprint("\tOutput path: ", block2dest_map_path)

        elif (step == 3):
            log.dprint("*******************************************")
            log.dprint("************* 3: Disaggregate *************")
            if (state == "CA" and year == 2018 and source_data_path != None and block2source_map_path != None and not isDemographicData):
                disaggregate_data_ca(state, stateCode, source_data_path, block2source_map_path, block_key, block_data_from_source_path)
            elif (source_data_path != None and block2source_map_path != None and block_pop_path != None and block_data_from_source_path != None):
                disaggregate_data(state, stateCode, source_data_path, source_key, block2source_map_path, block_key, block_pop_path, block_data_from_source_path, use_index_for_source_key, isDemographicData)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tSource data: ", source_data_path)
                log.dprint("\tBlock to source map: ", block2source_map_path)
                log.dprint("\tBlock population: ", block_pop_path)
                log.dprint("\tOutput path: ", block_data_from_source_path)

        elif (step == 4):
            log.dprint("*******************************************")
            log.dprint("*************** 4: Aggregate **************")
            if (block_data_from_source_path != None and block2dest_map_path != None and agg_data_from_source_path != None):
                aggregate_source2dest(state, stateCode, block_data_from_source_path, block2dest_map_path, dest_key, agg_data_from_source_path)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tBlock data: ", block_data_from_source_path)
                log.dprint("\tBlock to dest map: ", block2dest_map_path)
                log.dprint("\tOutput path: ", agg_data_from_source_path)

        elif (step == 5):        
            log.dprint("*******************************************")
            log.dprint("************* 5: Disaggregate *************")
            if (dest_data_path != None and block2dest_map_path != None and block_pop_path != None and block_data_from_dest_path != None):
                disaggregate_data(state, stateCode, dest_data_path, dest_key, block2dest_map_path, block_key, block_pop_path, block_data_from_dest_path)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tDest data: ", dest_data_path)
                log.dprint("\tBlock to dest map: ", block2dest_map_path)
                log.dprint("\tBlock population: ", block_pop_path)
                log.dprint("\tOutput path: ", block_data_from_dest_path)

        elif (step == 6):
            if (source_data_path != None and agg_data_from_source_path != None):
                log.dprint("*******************************************")
                log.dprint("**************** 6: Verify ****************")
                verify.verify_source_vs_aggregated(source_data_path, agg_data_from_source_path, ok_to_agg) #, block_data_from_source_path)
            else:
                log.dprint("Required input missing:")
                log.dprint("\tSource data: ", source_geo_path)
                log.dprint("\tAggregated data: ", agg_data_from_source_path)


# ****************************************************************
# Example Main

#try:
    #state = "GA"
    #prepare.prepare(state)
    #process_state(state, [1,2,3,4,6])
#finally:
    #log.close()