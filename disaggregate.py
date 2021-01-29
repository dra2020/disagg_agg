# Script to disaggregate data
#
# 2019
#


import geopandas as gpd
import json
import csv
from tqdm import tqdm
import numbers

def getf(ds, f, blk_pct):
    return 0 if not (f in ds) else(round(float(ds[f]) * blk_pct, 3))
    
def handle_datasets(blk, blk_pct, datasets):
    # {"D10F": {"Asn": 2, ...}, "D10T": {"Asn": 3, ....}}
    if "D10F" in datasets:
        d10f = datasets["D10F"]
        blk["TOT"] = getf(d10f, "Tot", blk_pct)
        blk["WH"] = getf(d10f, "Wh", blk_pct)
        blk["BL"] = getf(d10f, "Bl", blk_pct)
        blk["ASN"] = getf(d10f, "Asn", blk_pct)
        blk["NAT"] = getf(d10f, "Nat", blk_pct)
        blk["PAC"] = getf(d10f, "PI", blk_pct)
        blk["OTH"] = getf(d10f, "OthAl", blk_pct)
        blk["MIX"] = getf(d10f, "Mix", blk_pct)
        blk["HIS"] = getf(d10f, "His", blk_pct)
        blk["BLC"] = getf(d10f, "BlC", blk_pct)
        blk["ASNC"] = getf(d10f, "AsnC", blk_pct)
        blk["PACC"] = getf(d10f, "PacC", blk_pct)
        blk["NATC"] = getf(d10f, "NatC", blk_pct)
    if "D10T" in datasets:
        d10t = datasets["D10T"]
        blk["TOT18"] = getf(d10t, "Tot", blk_pct)
        blk["WH18"] = getf(d10t, "Wh", blk_pct)
        blk["BL18"] = getf(d10t, "Bl", blk_pct)
        blk["ASN18"] = getf(d10t, "Asn", blk_pct)
        blk["NAT18"] = getf(d10t, "Nat", blk_pct)
        blk["PAC18"] = getf(d10t, "PI", blk_pct)
        blk["OTH18"] = getf(d10t, "OthAl", blk_pct)
        blk["MIX18"] = getf(d10t, "Mix", blk_pct)
        blk["HIS18"] = getf(d10t, "His", blk_pct)
        blk["BLC18"] = getf(d10t, "BlC", blk_pct)
        blk["ASNC18"] = getf(d10t, "AsnC", blk_pct)
        blk["PACC18"] = getf(d10t, "PacC", blk_pct)
        blk["NATC18"] = getf(d10t, "NatC", blk_pct)


def make_block_props_map(log, source_props_path, block_map_path, block_pop_map, source_key, use_index_for_source_key, ok_to_agg):
    """
        source_props is geojson or shapefile
        block_map {blkid: source_key, ...}
        source_key is key field for source_props; value used to lookup in block_map; we always treat it as a string
        block_pop_map is either
            {blkid: population, ...} or
            {blkid: {'TOT': 3.0, 'ASN': 1.0, ..., 'TOT18': 2.0}}
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
                block_population = block_pop_map[key]
                error_block_population = block_population["TOT"] if isinstance(block_population, dict) else block_population
                if error_block_population > 0:
                    log.dprint("Block not in source: ", key, ", Pop: ", error_block_population)
                    count_blocks_not_in_source_with_nonzero_pop += 1
            else:
                sb_map_key = int(value) if use_index_for_source_key else value
                if sb_map_key in src_blk_map:
                    source_item = src_blk_map[sb_map_key]
                    source_item[1].append((key, block_pop_map[key]))
        else:
            log.dprint("Block not in block_pop_map: ", key)

    log.dprint("Number of blocks not in any source: ", count_blocks_not_in_source) 
    log.dprint("Number of blocks not in any source with population: ", count_blocks_not_in_source_with_nonzero_pop) 

    log.dprint("Build block to fields map (disaggregate)")
    print("Build block to fields map (disaggregate)")
    final_blk_map = {}          # {blkid: {prop1: val1, prop2: val2, ...}, ...}
    failed_props_set = set()    # For logging
    for src_value in tqdm(src_blk_map.values()):
        sum_blk_pop = 0
        for blk_tuple in src_value[1]:
            sum_blk_pop += blk_tuple[1]["TOT18"] if isinstance(blk_tuple[1], dict) else blk_tuple[1]

        for blk_tuple in src_value[1]:
            blk_pop = blk_tuple[1]["TOT18"] if isinstance(blk_tuple[1], dict) else blk_tuple[1]
            blk_pct = 0 if (sum_blk_pop == 0) else (blk_pop / sum_blk_pop)
            one_blk = {}
            source_props_item = source_props.loc[src_value[0]]
            for prop_key, prop_value in source_props_item.items():
                if prop_key == "datasets":
                    handle_datasets(one_blk, blk_pct, prop_value)
                elif ok_to_agg(prop_key):   # isinstance(prop_value, numbers.Number)
                    try:
                        # add float/int props only
                        one_blk[prop_key] = round(float(prop_value) * blk_pct, 3)
                    except:
                        failed_props_set.add(prop_key)

            final_blk_map[blk_tuple[0]] = one_blk
    
    if not bool(failed_props_set):
        log.dprint("For some rows, these props could not convert to float: ", failed_props_set)
    return final_blk_map


def keep_key(key):
    return key[0:3] == "ATG" or key[0:3] == "GOV" or key[0:3] == "USS" or key[0:3] == "LTG" or key[0:3] == "PRS"

def make_block_props_map_ca(log, source_props_path, block_map_path):
    """
        source_props is CSV [COUNTY, SRPREC, props...]
        block_map is CSV [COUNTY, ..., BLOCK_KEY, SRPREC, ]
        source_key is (COUNTY, SRPREC)

        Algorithm:
          for each row in block_map
            get row(COUNTY, SRPREC) from source_props
            for each interesting prop
              allocate PCTSRPREC % of value to BLOCK_KEY (note that blocks may be split across precincts)
    """
    source_props_map = {}
    with open(source_props_path) as source_csv_file:
        source_rows = csv.DictReader(source_csv_file, delimiter=",")
        for row in tqdm(source_rows):
            source_props_map[(row["COUNTY"], row["SRPREC"])] = row

    log.dprint("Build block to fields map (disaggregate)")
    print("Build block to fields map (disaggregate)")
    final_blk_map = {}              # {blkid: {prop1: val1, prop2: val2, ...}, ...}
    with open(block_map_path) as block_csv_file:
        block_map = csv.DictReader(block_csv_file, delimiter=",")
        for row in tqdm(block_map):
            county = row["COUNTY"]
            block = row["BLOCK_KEY"]
            srprec = row["SRPREC"]
            if srprec != "":        # skip first nan row
                pctsrprec = float(row["PCTSRPREC"]) / 100

                if not (block in final_blk_map):
                    final_blk_map[block] = {}
                if (county, srprec) in source_props_map:
                    row = source_props_map[(county, srprec)]
                    for key, value in row.items():
                        if keep_key(key):
                            blkval = float(value) * pctsrprec
                            if not (key in final_blk_map[block]):
                                final_blk_map[block][key] = blkval
                            else:
                                final_blk_map[block][key] += blkval

    return final_blk_map