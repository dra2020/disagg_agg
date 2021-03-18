
# Below Census directory is a folder for each state named its 2-letter code
#				Each state folder should have <state name>_Tracts_Block_Groups_Only (unzipped file from Census)
#				templates folder should have templates from Census
# Template headers: 2016_SFGeoFileTemplateHeader.csv, Seq5header.csv
# infiles found in <state name>_Tracts_Block_Groups_Only, which is in directory <state code>
# infile1: g20165XX.csv, where XX = state code
# infile2: e20165XX0005000.txt
# outfile1: 2016_XX_geo.csv
# outfile2: 2016_XX_data.csv

"""
New for 2018 ACS:
Below Census directory is a folder for each state named its 2-letter code
				Each state folder should have /2018/ACS/<state name>_Tracts_Block_Groups_Only.zip
        (or already unzipped into INPUT folder /2018/ACS/<state name>_Tracts_Block_Groups_Only)
				Census/2018templates folder should have templates from Census (for reference only)
 Template headers: 2018_SFGeoFileTemplateHeader.xlsx, Seq5.xlsx (for reference only)

 infiles found in INPUT folder
 infile1: g20185XX.csv, where XX = state code (lower case)
 infile2: e20185XX0005000.txt

"""

import pandas as pd
import json
import csv
import pprint
import os
import zipfile

import statecodes

def add_seq5_fields(rec_map, logrecno, row):
    total = row[37]    # B03002_001
    white = row[39]    # B03002_003
    black = row[40]    # B03002_004
    native = row[41]   # B03002_005
    asian = row[42]    # B03002_006
    pacific = row[43]  # B03002_007
    other = row[44]    # B03002_008
    mixed = row[45]    # B03002_009
    hisp = row[48]     # B03002_012

    rec_map[logrecno]["TOT"] = total
    rec_map[logrecno]["WH"] = white
    rec_map[logrecno]["BL"] = black
    rec_map[logrecno]["NAT"] = native
    rec_map[logrecno]["ASN"] = asian
    rec_map[logrecno]["PAC"] = pacific
    rec_map[logrecno]["OTH"] = other
    rec_map[logrecno]["MIX"] = mixed
    rec_map[logrecno]["HIS"] = hisp


def add_seq4_fields(rec_map, logrecno, row):    
    black_incl_combo = row[17]    # B02009_001
    native_incl_combo = row[18]   # B02010_001
    asian_incl_combo = row[19]    # B02011_001
    pacific_incl_combo = row[20]  # B02012_001

    rec_map[logrecno]["BLC"] = black_incl_combo
    rec_map[logrecno]["NATC"] = native_incl_combo
    rec_map[logrecno]["ASNC"] = asian_incl_combo
    rec_map[logrecno]["PACC"] = pacific_incl_combo


def add_seq8_fields(rec_map, logrecno, row):    
    total18 = row[52] + row[53]   # B05003_008 + B05003_019
    rec_map[logrecno]["TOT18"] = total18


def add_seq9_fields(rec_map, logrecno, row):    
    white18 = row[13] + row[24]   # B05003H_008 + B05003H_019
    hisp18 = row[36] + row[47]    # B05003I_008 + B05003I_019
    rec_map[logrecno]["WH18"] = white18
    rec_map[logrecno]["HIS18"] = hisp18




def extract(state):
    """
    Geo file has these columns (that we care about); we need (STATE,COUNTY,TRACT,BLKGRP) to identify a block group; and LOGRECNO as key to Data file
    FILEID	STUSAB	SUMLEVEL	COMPONENT	LOGRECNO	US	REGION	DIVISION	STATECE	STATE	COUNTY	COUSUB	PLACE	TRACT	BLKGRP

    Data file has these columns + all of the actual data columns
    FILEID	FILETYPE	STUSAB	CHARITER	SEQUENCE	LOGRECNO
    """

    year = "2019"
    in_root = "../Documents/Redist/Census/"
    stateCode = statecodes.make_state_codes()[state]
    stateName = statecodes.make_state_names_nospace()[state]

    stateDir = stateName + "_Tracts_Block_Groups_Only"
    geo_prefix = "/g" + year + "5"
    data_prefix = "/e" + year + "5"
    input_dir = in_root + state + "/" + year + "/ACS/" + stateDir

    zip_path = input_dir + ".zip"
    if not os.path.exists(input_dir):
        os.mkdir(input_dir)
        print("Unzip ", zip_path, " ==> ", input_dir)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(input_dir)
    else:
        print(zip_path, " already unzipped: ", input_dir)

    infile_geo_path = input_dir + geo_prefix + state.lower() + ".csv"
    infile_seq4_path = input_dir + data_prefix + state.lower() + "0004000.txt"
    infile_seq5_path = input_dir + data_prefix + state.lower() + "0005000.txt"
    infile_seq8_path = input_dir + data_prefix + state.lower() + "0008000.txt"
    infile_seq9_path = input_dir + data_prefix + state.lower() + "0009000.txt"

    outfile_path = in_root + state + "/" + year + "/" + year + "ACS_bg_" + stateCode + ".json"

    print("Processing " + year + " ACS data for " + stateCode)
    print("Input geo: ", infile_geo_path)
    print("Input seq4: ", infile_seq4_path)
    print("Input seq5: ", infile_seq5_path)
    print("Input seq8: ", infile_seq8_path)
    print("Input seq9: ", infile_seq9_path)
    print("Output: ", outfile_path)

    rec_map = {}
    csv_row_count = 0
    with open(infile_geo_path) as geo_file:
        geo_data = csv.reader(geo_file, delimiter=",")
        for row in geo_data:
            if row[2] == "150":     #SUMLEVEL = 150 means Block Group
                # STATE+COUNTY+TRACT+BLKGRP
                geoid = row[9] + row[10] + row[13] + row[14]
                rec_map[row[4]] = {"GEOID" : geoid}
            csv_row_count += 1
            #print(csv_row_count)

    # Pull only rows with LOGRECNO's (column 5) in the map
    with open(infile_seq4_path) as seq4_file:
        seq4_data = csv.reader(seq4_file, delimiter=",")
        for row in seq4_data:
            if row[5] in rec_map:
                add_seq4_fields(rec_map, row[5], row)

    # Pull only rows with LOGRECNO's (column 5) in the map
    with open(infile_seq5_path) as seq5_file:
        seq5_data = csv.reader(seq5_file, delimiter=",")
        for row in seq5_data:
            if row[5] in rec_map:
                add_seq5_fields(rec_map, row[5], row)

    """
    NOT AVALIABLE AT BLOCKGROUP LEVEL
    # Pull only rows with LOGRECNO's (column 5) in the map
    with open(infile_seq8_path) as seq8_file:
        seq8_data = csv.reader(seq8_file, delimiter=",")
        for row in seq8_data:
            if row[5] in rec_map:
                add_seq8_fields(rec_map, row[5], row)

    # Pull only rows with LOGRECNO's (column 5) in the map
    with open(infile_seq9_path) as seq9_file:
        seq9_data = csv.reader(seq9_file, delimiter=",")
        for row in seq9_data:
            if row[5] in rec_map:
                add_seq9_fields(rec_map, row[5], row)
    """

    #pp = pprint.PrettyPrinter(indent=4)
    #print(rec_map)

    print("Build geojson")
    features = []
    for props in rec_map.values():
        features.append({'type': 'Feature', 'geometry': None, 'properties': props})

    with open(outfile_path, 'w') as outf:
        json.dump({'type': 'FeatureCollection', 'features': features}, outf, ensure_ascii=False)


# Main

#for state in ["CT","IA","ID","IN","KY","LA","ME","MS","MT","NY","OH","OK","OR","PA","SD","UT","WA","WV","WY"]:  
#for state in ["DE","FL","GA","HI","IL","KS","MA","MD","MI","MN","MO","NH","RI","SC","TN","VT","WI"]:         
#for state in ["AL","ND","NE","NJ","NV"]:
#for state in ["AZ","CO","TX","NM","CA"]:   # these fail because of the funky ñ
#for state in ["AK","AR"]:
for state in ["OK"]:
    extract(state)
