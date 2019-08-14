# Example prepare_disagg_agg.py
#
#

import csv
import zipfile
import os
import json

import statecodes

# *********************************************************
# Helpers
# *********************************************************

def make_block_pop_map18(state, stateCode, block_pop_csv_path, out_root):
    """
    Given CSV with fields: 
        GEOID,STATE,SUMLEV,STATE (FIPS),COUNTY,TRACT,BG,BLOCK,CD,VTD,NAME,
          TOTAL,WHITE,BLACK,NATIVE,ASIAN,PACIFIC,OTHER,MIXED,HISPANIC,
          TOTAL18,WHITE18,BLACK18,NATIVE18,ASIAN18,PACIFIC18,OTHER18,MIXED18,HISPANIC18
        make a map of GEOID (formed from those fields) -> Total18 (total voting age population)
    """
    block_pop_map = {}
    csv_row_count = 0
    totalPop = 0
    with open(block_pop_csv_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=",")
        for row in csv_reader:
            if csv_row_count > 0:
                geoid = row[0]
                value = float(row[20])
                block_pop_map[geoid] = value
                totalPop += value
            csv_row_count += 1

    print("Num blocks: ", csv_row_count - 1, ", Total Voting Age Pop: ", totalPop)
    block_map_path = get_made_file_paths(state, stateCode, out_root)[3]
    print("Write block map ", block_map_path)
    with open(block_map_path, "w") as outf:
        json.dump(block_pop_map, outf, ensure_ascii=False)


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

def get_paths(state):
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


def get_keys(state):
    block_key = "GEOID10"
    dest_key = "GEOID10"
    source_key = None
    use_index_for_source_key = False

    if state == "VA":
        source_key = "loc_prec"
    else:
        use_index_for_source_key = True       # source_geo and source_data must be the same to use index

    return source_key, dest_key, block_key, use_index_for_source_key


def prepare(state):
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

    block_pop_csv_path = in_root + state + "/" + state.lower() + "2010-TABBLOCK.csv"
    make_block_pop_map18(state, stateCode, block_pop_csv_path, out_root)
