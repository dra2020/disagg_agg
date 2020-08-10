# Example prepare_disagg_agg.py
#
#

import csv
import zipfile
import os
import json

from . import statecodes

# *********************************************************
# Helpers
# *********************************************************

def unzip_shapes(zip_path, shapes_path):
    if (not os.path.exists(shapes_path)):
        os.mkdir(shapes_path)
    print("Unzip ", zip_path, " ==> ", shapes_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(shapes_path)


def unzip_dest_shapes(state, stateCode, zip_path, out_root):  
    dest_shapes_path = get_made_file_paths(state, stateCode, out_root)[1]
    unzip_shapes(zip_path, dest_shapes_path)


def unzip_block_shapes(state, stateCode, zip_path, out_root):
    block_shapes_path = get_made_file_paths(state, stateCode, out_root)[2]
    unzip_shapes(zip_path, block_shapes_path)


def get_made_file_paths(state, stateCode, out_root):
    working_path = out_root + state + '/agg_working/'
    dest_shapes_path = working_path + "dest_shapes/"
    block_shapes_path = working_path + "block_shapes/"
    block_map_path = working_path + "block_pop_map" + stateCode + ".json"
    source_shapes_path = working_path + "source_shapes/"            # if needed
    return working_path, dest_shapes_path, block_shapes_path, block_map_path, source_shapes_path



# *********************************************************
# Externally called functions
# *********************************************************

def get_paths(state, isElections=True, year=2016, isCVAP=False):
    stateCode = statecodes.make_state_codes()[state]
    in_root = '../Documents/Census/'
    out_root = '../Documents/Census/'
    sourceid = '2016prec'
    destid = '2010prec'

    working_path, dest_shapes_path, block_shapes_path, block_pop_path, source_shapes_path = get_made_file_paths(state, stateCode, out_root)

    if state == "VA":
        source_geo_path = in_root + state + '/2016/VA_Precincts_CD_and_Elections.geojson'
    else:
        source_geo_path = source_shapes_path + state.lower() + "_2016.shp"

    source_data_path = source_geo_path
    block_geo_path = block_shapes_path + 'tl_2010_' + stateCode + '_tabblock10.shp'
    dest_geo_path = dest_shapes_path + 'vt' + stateCode + '.shp'
    dest_data_path = in_root + state + '2010/vt' + stateCode + '_data.json'

    block2source_map_path = working_path + 'block_to_' + sourceid + '_map_' + stateCode + '.json'
    block2dest_map_path = working_path + 'block_to_' + destid + '_map_' + stateCode + '.json'
    block_data_from_source_path = working_path + 'block_data_from_' + sourceid + '_' + stateCode + '.json'
    block_data_from_dest_path = working_path + 'block_data_from_' + destid + '_' + stateCode + '.json'
    agg_data_from_source_path = working_path + destid + '_data_from_' + sourceid + '_' + stateCode + '.json'

    return {
        "source_geo_path": source_geo_path,
        "source_data_path": source_data_path,
        "block_geo_path": block_geo_path,
        "dest_geo_path": dest_geo_path,
        "dest_data_path": dest_data_path,
        "block_pop_path": block_pop_path,
        "block2source_map_path": block2source_map_path,
        "block2dest_map_path": block2dest_map_path,
        "block_data_from_source_path": block_data_from_source_path,
        "block_data_from_dest_path": block_data_from_dest_path,
        "agg_data_from_source_path": agg_data_from_source_path,
        "working_path": working_path
    }


def get_keys(state, isElections, year):
    block_key = "GEOID10"
    dest_key = "GEOID10"
    source_key = None
    use_index_for_source_key = False

    if state == "VA":
        source_key = "loc_prec"
    else:
        use_index_for_source_key = True       # source_geo and source_data must be the same to use index

    return source_key, dest_key, block_key, use_index_for_source_key


def prepare(state, isElections=True):
    stateCode = statecodes.make_state_codes()[state]
    in_root = "./inputs/"
    out_root = "../Documents/Census/"

    working_path = get_made_file_paths(state, stateCode, out_root)[0]
    if (not os.path.exists(working_path)):
        os.mkdir(working_path)

    vt_shapes_zip_path = in_root + state + "/vtd_2010_" + stateCode + "_" + state + ".zip"
    unzip_dest_shapes(state, stateCode, vt_shapes_zip_path, out_root)

    block_shapes_zip_path = out_root + state + "/2010/tl_2010_" + stateCode + "_tabblock10.zip"
    unzip_block_shapes(state, stateCode, block_shapes_zip_path, out_root)

