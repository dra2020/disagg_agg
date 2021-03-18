"""
2018 CVAP

Input:
    BlockGr.csv (for all states) with fields
    geoname, Intitle, geoid, Innumber, cit_est, cit_moe, cvap_est, cvap_moe

    Each row represents 1 demographic group, so there are 13 rows per block group
        1 Total
        2 Not Hispanic or Latino
          3 American Indian or Alaska Native Alone
          4 Asian Alone
          5 Black or African American Alone
          6 Native Hawaiian or Other Pacific Islander Alone
          7 White Alone
          8 American Indian or Alaska Native and White
          9 Asian and White
          10 Black or African American and White
          11 American Indian or Alaska Native and Black or African American
          12 Remainder of Two or More Race Responses
        13 Hispanic or Latino

    geoid has fixed prefix 15000US and then
    bgid == nncccddddddd, where nn=state code, ccc=county code, ddddddd= 7-digit bgid
    
    Result is geojson with fields
      GEOID: (12-digit)
      TOT(1), WH(7), BL(5), NAT(3), ASN(4), PAC(6), OTH(12), HIS(13), BLC(5+10+11), NATC(3+8+11), ASNC(4+9), PACC(6)

Output:
    For each state: SS/2018/2018CVAP_bg_nn.json

"""

from tqdm import tqdm
import json
import csv
import pprint
import os

import statecodes

def add_fields(rec_map, bgid, row_in_bg, value_str):
    value = int(value_str)
    if row_in_bg == 1:
        rec_map[bgid]["TOT"] = value
    elif row_in_bg == 3:
        rec_map[bgid]["NAT"] = value
        rec_map[bgid]["NATC"] = value
    elif row_in_bg == 4:
        rec_map[bgid]["ASN"] = value
        rec_map[bgid]["ASNC"] = value
    elif row_in_bg == 5:
        rec_map[bgid]["BL"] = value
        rec_map[bgid]["BLC"] = value
    elif row_in_bg == 6:
        rec_map[bgid]["PAC"] = value
        rec_map[bgid]["PACC"] = value
    elif row_in_bg == 7:
        rec_map[bgid]["WH"] = value
    elif row_in_bg == 8:
        rec_map[bgid]["NATC"] += value
    elif row_in_bg == 9:
        rec_map[bgid]["ASNC"] += value
    elif row_in_bg == 10:
        rec_map[bgid]["BLC"] += value
    elif row_in_bg == 11:
        rec_map[bgid]["BLC"] += value
        rec_map[bgid]["NATC"] += value
    elif row_in_bg == 12:
        rec_map[bgid]["OTH"] = value
    elif row_in_bg == 13:
        rec_map[bgid]["HIS"] = value

def output_state(in_path, state, stateCode, year, rec_map):
    outfile_path = in_path + state + "/" + year + "/" + year + "CVAP_bg_" + stateCode + ".json"

    print("Output: ", outfile_path)
    print("Build geojson")
    features = []
    for props in rec_map.values():
        features.append({'type': 'Feature', 'geometry': None, 'properties': props})

    with open(outfile_path, 'w') as outf:
        json.dump({'type': 'FeatureCollection', 'features': features}, outf, ensure_ascii=False)


def extract():

    skip_until = 0
    year = "2018"
    in_path = "../Documents/Redist/Census/"
    in_file_path = in_path + "BlockGr.csv"
    state_code_lookup = statecodes.make_state_digit_to_code()

    print("Processing " + year + " CVAP data")
    print("Input:", in_file_path)

    rec_map = {}  # {"geoid": {"WH": number, "BL": number, ...}}
    current_state_code = ""
    row_in_bg = 1
    current_bgid = ""
    state_bg_count = 0
    with open(in_file_path) as in_file:
        bgdata = csv.reader(in_file, delimiter=",")

        for row in tqdm(bgdata):
            if row[0] == "geoname":
                continue  # Skip 1st row
            bgid = row[2][7:]
            state_code = bgid[0:2]
            if int(state_code) < skip_until:
                continue
            if current_state_code != state_code:
                if current_state_code != "":
                    # output state
                    state = state_code_lookup[current_state_code]
                    print("Output state:", state, "(" + current_state_code + "); BG count:", state_bg_count)
                    output_state(in_path, state, current_state_code, year, rec_map)
                current_state_code = state_code
                rec_map = {}  # reset
                state_bg_count = 0
                current_bgid = ""
            if current_bgid != bgid:
                rec_map[bgid] = {"GEOID": bgid}
                current_bgid = bgid
                row_in_bg = 1
                state_bg_count += 1
            add_fields(rec_map, bgid, row_in_bg, row[6])
            row_in_bg += 1


# Main
extract()
            
                
            