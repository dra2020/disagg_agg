
# Process the 2010 redistricting files and produce 2010Census_vt_NN.json
#	This script will extract the voting district rows (code 700)

# Inputs	XXgeo2010.pl XX000012010.pl XX000022010.pl (unzipped from XX2010.pl)

import os
import zipfile
import pandas as pd
import json
import csv
import pprint
import traceback

import statecodes

def unzip_pkg(zip_path, pkg_temp_path):
    if (not os.path.exists(pkg_temp_path)):
        os.mkdir(pkg_temp_path)
    print("Unzip ", zip_path, " ==> ", pkg_temp_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(pkg_temp_path)

def extract(state):

    out_temp_root = "../Documents/Redist/Census/"
    in_root = "../Documents/Redist/Temp/"
    stateCode = statecodes.make_state_codes()[state]

    outfile_path = out_temp_root + state + "/2010/" + "2010Census_vt_" + stateCode + ".json"

    try:
        zip_path = in_root + state.lower() + "2010.pl.zip"
        pkg_temp_path = out_temp_root + state + "/temp/"
        geo_path = pkg_temp_path + state.lower() + "geo2010.pl"
        sf1_path = pkg_temp_path + state.lower() + "000012010.pl"   # csv
        sf2_path = pkg_temp_path + state.lower() + "000022010.pl"   # csv
        if not (os.path.exists(geo_path) and os.path.exists(sf1_path) and os.path.exists(sf2_path)):
            unzip_pkg(zip_path, pkg_temp_path)

        # geo file has fixed-with fields
        rec_map = {}
        line = 0
        with open(geo_path, "rt") as geo_file:    # UTF-8
            for geo_line in geo_file:
                line += 1
                #print("Line ", line, ", Length: ", len(geo_line))
                sumlevel = geo_line[8:8 + 3]
                if sumlevel == "700":           # VTD
                    logrecno = geo_line[18:18 + 7]
                    st = geo_line[27:27 + 2]
                    county = geo_line[29:29 + 3]
                    vtd = geo_line[161:161 + 6].strip()
                    name = geo_line[226:226 + 90].strip()
                    rec_map[logrecno] = {"GEOID10": st + county + vtd, "name": name}
        
        with open(sf1_path) as sf1_file:
            sf1_data = csv.reader(sf1_file, delimiter=",")
            for row in sf1_data:
                logrecno = row[4]
                if logrecno in rec_map:
                    total =   int(row[75 + 1])  # P0020001
                    white =   int(row[75 + 5])  # P0020005
                    hisp =    int(row[75 + 2])  # P0020002
                    black =   int(row[75 + 6])  # P0020006
                    native =  int(row[75 + 7])  # P0020007
                    asian =   int(row[75 + 8])  # P0020008
                    pacific = int(row[75 + 9])  # P0020009
                    other =   int(row[75 + 10]) # P0020010
                    mixed =   int(row[75 + 11]) # P0020011
                    
                    rec_map[logrecno]["TOT"] = total
                    rec_map[logrecno]["WH"] = white
                    rec_map[logrecno]["BL"] = black
                    rec_map[logrecno]["NAT"] = native
                    rec_map[logrecno]["ASN"] = asian
                    rec_map[logrecno]["PAC"] = pacific
                    rec_map[logrecno]["OTH"] = other
                    rec_map[logrecno]["MIX"] = mixed
                    rec_map[logrecno]["HIS"] = hisp

        with open(sf2_path) as sf2_file:
            sf2_data = csv.reader(sf2_file, delimiter=",")
            for row in sf2_data:
                logrecno = row[4]
                if logrecno in rec_map:
                    total =   int(row[75 + 1])  # P0040001
                    white =   int(row[75 + 5])  # P0040005
                    hisp =    int(row[75 + 2])  # P0040002
                    black =   int(row[75 + 6])  # P0040006
                    native =  int(row[75 + 7])  # P0040007
                    asian =   int(row[75 + 8])  # P0040008
                    pacific = int(row[75 + 9])  # P0040009
                    other =   int(row[75 + 10]) # P0040010
                    mixed =   int(row[75 + 11]) # P0040011
                    
                    rec_map[logrecno]["TOT18"] = total
                    rec_map[logrecno]["WH18"] = white
                    rec_map[logrecno]["BL18"] = black
                    rec_map[logrecno]["NAT18"] = native
                    rec_map[logrecno]["ASN18"] = asian
                    rec_map[logrecno]["PAC18"] = pacific
                    rec_map[logrecno]["OTH18"] = other
                    rec_map[logrecno]["MIX18"] = mixed
                    rec_map[logrecno]["HIS18"] = hisp

        #print(rec_map)

        print("Build geojson for " + state)
        features = []
        for props in rec_map.values():
            features.append({'type': 'Feature', 'geometry': None, 'properties': props})

        with open(outfile_path, 'w') as outf:
            json.dump({'type': 'FeatureCollection', 'features': features}, outf, ensure_ascii=False)
    except:
        print("Extract failed: " + state)
        traceback.print_exc()


# Main

# Done:
extract("DC")
        


