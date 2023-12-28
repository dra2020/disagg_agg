# Script to disaggregate data
#
# 2019
#


import geopandas as gpd
import json
import csv
import math
from tqdm import tqdm
from fractions import Fraction

def getf(ds, f, blk_pct):
    return 0 if not (f in ds) else (round(float(ds[f]) * blk_pct, 3))
    
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

def sum_props(prop_totals_map, prop_key, prop_value):
    try:
        intval = int(prop_value)
        if not (prop_key in prop_totals_map):
            prop_totals_map[prop_key] = 0
        prop_totals_map[prop_key] += intval
    except:
        ignore = 0
    return

# TODO This really needs state and year (5/3/23)
def cong_party_wa(cand_code):
    consufx = cand_code[4:9]
    if (consufx == "01DEL" or consufx == "02LAR" or consufx == "03PER" or consufx == "04WHI" or consufx == "05HIL" or 
        consufx == "06KIL" or consufx == "07JAY" or consufx == "08SCH" or consufx == "09SMI" or consufx == "10STR"):
        return "DVAR"
    if (consufx == "01CAV" or consufx == "02MAT" or consufx == "03KEN" or consufx == "04NEW" or consufx == "05ROD" or 
        consufx == "06KRE" or consufx == "07MOO" or consufx == "08LAR" or consufx == "09BAS" or consufx == "10SWA"):
        return "RVAR"
    return "IOTH"

def cong_party_nc(cand_code):
    if cand_code[1:4] == "CON":     # Congress
        party = cand_code[6:7]
        return "DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH"
    if cand_code[1:4] == "SSC":     # State Supreme Court
        consufx = cand_code[4:9]
        if (consufx == "03INM" or consufx == "05ERV"):
            return "DVAR"
        if (consufx == "03DIE" or consufx == "05ALL"):
            return "RVAR"
        return "IOTH"
    return "UNK"

def filter_prop_key(cand_code, state):
    if state == "WA":
        """                 # Already done in extracting the data
        contest = None
        suffix = cand_code[6:]
        match cand_code[3:5]:
            case "SE": 
                contest = cand_code[0:3] + "SEN" + ("DMUR" if suffix.startswith("MUR") else "RSMI" if suffix.startswith("SMI") else "IOTH")
            case "SO":
                contest = cand_code[0:3] + "SOS" + ("DHOB" if suffix.startswith("HOB") else "IAND" if suffix.startswith("AND") else "ROTH")
            case "C0":
                contest = cand_code[0:3] + "CON" + cong_party_wa(cand_code)
            case "C1":
                contest = cand_code[0:3] + "CON" + cong_party_wa(cand_code)
        """
        return cand_code
    elif state == "NC":
        contest = None
        suffix = cand_code[6:]
        match cand_code[1:4]:
            case "22U": 
                contest = cand_code[0:3] + "SEN" + ("DBEA" if suffix.startswith("DBEA") else "RBUD" if suffix.startswith("RBUD") else "IOTH")
            case "CON":
                contest = "G22CON" + cong_party_nc(cand_code)
            case "SSC":
                contest = "G22SC" + ("3" if cand_code[4:6] == "03" else "5") + cong_party_nc(cand_code)
        return contest
    elif state == "WI":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22SOS":
                contest = "G22" + "SOS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22TRE":
                contest = "G22" + "TRE" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "MN":
        contest = None
        match cand_code[0:2]:
            case "MN":
                match cand_code[2:4]:
                    case "GO":
                        if cand_code[5] != "T":
                            contest = "G22" + "GOV" + ("DVAR" if cand_code[5] == "D" else "RVAR" if cand_code[5] == "R" else "IOTH")
                    case "SO":
                        if cand_code[5] != "T":
                            contest = "G22" + "SOS" + ("DVAR" if cand_code[5] == "D" else "RVAR" if cand_code[5] == "R" else "IOTH")
                    case "AU":
                        if cand_code[5] != "T":
                            contest = "G22" + "AUD" + ("DVAR" if cand_code[5] == "D" else "RVAR" if cand_code[5] == "R" else "IOTH")
                    case "AG":
                        if cand_code[4] != "T":
                            contest = "G22" + "ATG" + ("DVAR" if cand_code[4] == "D" else "RVAR" if cand_code[4] == "R" else "IOTH")
            case "US":
                if cand_code[2:5] == "REP" and cand_code[5] != "T":
                    contest = "G22" + "CON" + ("DVAR" if cand_code[5] == "D" else "RVAR" if cand_code[5] == "R" else "IOTH")
        return contest
    elif state == "TX":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22LTG":
                contest = "G22" + "LTG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22COM":
                contest = "G22" + "TRE" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "AK":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22CON":
                contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "LA":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "HI":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "OH":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22SOS":
                contest = "G22" + "SOS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22TRE":
                contest = "G22" + "TRE" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22AUD":
                contest = "G22" + "AUD" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22JUS":
                if cand_code[7:10] == "FIS" or cand_code[7:10] == "JAM":
                    contest = "G22" + "SC1" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
                else:
                    contest = "G22" + "SC2" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22CJU":
                contest = "G22" + "SCC" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "AL":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22SOS":
                contest = "G22" + "SOS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22LTG":
                contest = "G22" + "LTG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22AUD":
                contest = "G22" + "AUD" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22AJ5":
                contest = "G22" + "SC5" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22AJ6":
                contest = "G22" + "SC6" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "MT":
        contest = None
        party = cand_code[5:6]
        if cand_code[0:4] == "GCON":
            contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "GA":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22SOS":
                contest = "G22" + "SOS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22LTG":
                contest = "G22" + "LTG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest
    elif state == "FL":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22USS":
                contest = "G22" + "USS" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22CFO":
                contest = "G22" + "TRE" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case "G22ATG":
                contest = "G22" + "ATG" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
            case other:
                if cand_code[0:4] == "GCON":
                    contest = "G22" + "CON" + ("DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH")
        return contest

    return cand_code

def make_block_props_map_old(log, source_props_path, block_map_path, block_pop_map, source_key, use_index_for_source_key, ok_to_agg):
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

def make_block_props_map(log, source_props_path, block_map_path, block_pop_map, source_key, use_index_for_source_key, ok_to_agg, state):
    """
        source_props is geojson or shapefile
        block_map {blkid: [source_key, ...], ...}
        source_key is key field for source_props; value used to lookup in block_map; we always treat it as a string
        block_pop_map is either
            {blkid: population, ...} or
        use_index_for_source_key: True ==> Use geopandas index as key
    """
    with open(block_map_path) as json_file:
        block_map = json.load(json_file)

        source_props = gpd.read_file(source_props_path)
        source_props_total = {}

        # Build source props map {srckey1: {prop1: val1, ...}, ...}
        source_props_map = {}
        for i in source_props.index:
            source_props_item = source_props.loc[i]
            srckey = i if use_index_for_source_key else str(source_props_item[source_key])
            source_props_map[srckey] = {}
            for prop_key, prop_value in source_props_item.items():
                if prop_key != srckey and ok_to_agg(prop_key, prop_value):
                    # To handle more contests, call
                    prop_key = filter_prop_key(prop_key, state)
                    if prop_key != None:
                        if not (prop_key in source_props_map[srckey]):
                            source_props_map[srckey][prop_key] = 0
                        source_props_map[srckey][prop_key] += int(prop_value)
                        sum_props(source_props_total, prop_key, prop_value)
                    

        pp = log.pretty_printer()
        log.dprint("Source Props totals")
        pp.pprint(source_props_total)        

        # Build map {prec1: {blk1: pct1, ...}, ...}
        # Then build map {prec1: {blk1: {key1: val1, ...}, ...}, ...} with largest_remainder for all keys on all precincts, so all values are integers
        prec_blk_pct_map = {}
        for block, preclist in tqdm(block_map.items()):
            if isinstance(preclist, list):
                for prec in preclist:
                    if prec != "":
                        if not (prec in prec_blk_pct_map):
                            prec_blk_pct_map[prec] = {}
                        if not (block in prec_blk_pct_map[prec]):
                            prec_blk_pct_map[prec][block] = 0
            else:
                prec = preclist
                if prec != "":
                    if not (prec in prec_blk_pct_map):
                        prec_blk_pct_map[prec] = {}
                    if not (block in prec_blk_pct_map[prec]):
                        prec_blk_pct_map[prec][block] = 0

        for prec, blks in prec_blk_pct_map.items():
            sum_blks_pop = 0
            for block in blks.keys():
                if block in block_pop_map:
                    sum_blks_pop += block_pop_map[block]
                else:
                    log.dprint("Block not in block_pop_map: ", block)
            if sum_blks_pop > 0:
                for block in blks.keys():
                    if block in block_pop_map:
                          blks[block] = block_pop_map[block] / sum_blks_pop
            else:
                for block in blks.keys():
                    blks[block] = 1     # All blocks have zero pop, but pick one in case prec has data
                    break

        prec_blk_key_map = make_prec_blk_key_map(log, prec_blk_pct_map, source_props_map, ok_to_agg)
        return make_final_blk_map(log, prec_blk_key_map)


"""
We use California state database's SRPREC csv data file for the data.
These files use a non-FIPS count code, so we need to translate to build a proper SRPREC_KEY
"""

def srprec_key(county, srprec):
    countyfp = str(int(county) * 2 - 1).zfill(3)
    return "06" + countyfp + str(srprec)

def keep_key(key):
    return key[0:3] == "ATG" or key[0:3] == "GOV" or key[0:3] == "USS" or key[0:3] == "LTG" or key[0:3] == "PRS"

def make_block_props_map_ca(log, source_props_path, block_map_path):
    """
        source_props is CSV [COUNTY, SRPREC, props...]   or may have SRPREC_KEY instead of COUNTY
        block_map is CSV [COUNTY, ..., BLOCK_KEY, SRPREC, ]
        source_key is (COUNTY, SRPREC)

        Algorithm:
          for each row in block_map
            get row(COUNTY, SRPREC) from source_props
            for each interesting prop
              allocate PCTSRPREC % of value to BLOCK_KEY (note that blocks may be split across precincts)
    """
    source_props_map = {}
    with open(source_props_path) as source_csv_file, open(block_map_path) as block_csv_file:
        source_rows = csv.DictReader(source_csv_file, delimiter=",")
        for row in tqdm(source_rows):
            srprec = ""
            if "SRPREC_KEY" in row:
                srprec = row["SRPREC_KEY"]
            elif "COUNTY" in row:
                if row["COUNTY"] == "CNTYTOT":
                    continue
                srprec = srprec_key(row["COUNTY"], row["SRPREC"])
            source_props_map[srprec] = row

        # Build map {prec1: {blk1: pct1, ...}, ...}
        # Then build map {prec1: {blk1: {key1: val1, ...}, ...}, ...} with largest_remainder for all keys on all precincts, so all values are integers
        # A block may appear in more than 1 prec list
        block_map = csv.DictReader(block_csv_file, delimiter=",")

        prec_blk_pct_map = {}
        for row in tqdm(block_map):
            block = row["BLOCK_KEY"]
            srprec = row["SRPREC_KEY"]
            if srprec != "":        # skip first nan row
                pctsrprec = float(row["PCTSRPREC"]) / 100
                if not (srprec in prec_blk_pct_map):
                    prec_blk_pct_map[srprec] = {}
                if not (block in prec_blk_pct_map[srprec]):
                    prec_blk_pct_map[srprec][block] = 0
                prec_blk_pct_map[srprec][block] += pctsrprec
        # verify_pcts()
        prec_blk_key_map = make_prec_blk_key_map(log, prec_blk_pct_map, source_props_map, keep_key)
        return make_final_blk_map(log, prec_blk_key_map)

def make_prec_blk_key_map(log, prec_blk_pct_map, source_props_map, ok_to_agg):
    prec_blk_key_map = {}
    cant_disagg_set = {}

    all_precs = set({})
    for srprec in source_props_map.keys():
        all_precs.add(srprec)

    props_total = {}
    seen_precs = set({})
    for srprec, blk_pcts in prec_blk_pct_map.items():
        if srprec in source_props_map:
            seen_precs.add(srprec)
            prec_blk_key_map[srprec] = {}
            for blk in blk_pcts.keys():
                prec_blk_key_map[srprec][blk] = {}
            row = source_props_map[srprec]
            for key, value in row.items():
                if key == "datasets":
                    distribute_dataset_values(prec_blk_key_map[srprec], blk_pcts, value)
                elif ok_to_agg(key):
                    try:
                        distribute_value(prec_blk_key_map[srprec], blk_pcts, key, int(value), srprec)
                        sum_props(props_total, key, value)
                    except:
                        if not (key in cant_disagg_set):
                            print("Can't disagg key: ", key)
                            cant_disagg_set[key] = True

        else:
            log.dprint("Prec Key not found: ", srprec)

    print("Left out precs", all_precs - seen_precs, sep=" ")
    pp = log.pretty_printer()
    log.dprint("Props totals from Distributing")
    pp.pprint(props_total)        
    return prec_blk_key_map

def getf(ds, f):
    return 0 if not (f in ds) else (ds[f])

def distribute_dataset_values(blk_key_map, blk_pcts, datasets):
    if "D10F" in datasets:
        ds = datasets["D10F"]
        distribute_value(blk_key_map, blk_pcts, "TOT", getf(ds, "Tot"))
        distribute_value(blk_key_map, blk_pcts, "WH", getf(ds, "Wh"))
        distribute_value(blk_key_map, blk_pcts, "HIS", getf(ds, "His"))
        distribute_value(blk_key_map, blk_pcts, "BLC", getf(ds, "BlC"))
        distribute_value(blk_key_map, blk_pcts, "ASNC", getf(ds, "AsnC"))
        distribute_value(blk_key_map, blk_pcts, "PACC", getf(ds, "PacC"))
        distribute_value(blk_key_map, blk_pcts, "NATC", getf(ds, "NatC"))
    if "D10T" in datasets:
        ds = datasets["D10T"]
        distribute_value(blk_key_map, blk_pcts, "TOT18", getf(ds, "Tot"))
        distribute_value(blk_key_map, blk_pcts, "WH18", getf(ds, "Wh"))
        distribute_value(blk_key_map, blk_pcts, "HIS18", getf(ds, "His"))
        distribute_value(blk_key_map, blk_pcts, "BLC18", getf(ds, "BlC"))
        distribute_value(blk_key_map, blk_pcts, "ASNC18", getf(ds, "AsnC"))
        distribute_value(blk_key_map, blk_pcts, "PACC18", getf(ds, "PacC"))
        distribute_value(blk_key_map, blk_pcts, "NATC18", getf(ds, "NatC"))

def distribute_value(blk_key_map, blk_pcts, key, value, prec=''):
    # Hare quota (Hamilton)

    blks_info = []    # [(blk, whole, rem), ...]
    sum_wholes = 0
    for blk, pct in blk_pcts.items():
        whole = math.floor(value * pct)
        sum_wholes += whole
        blks_info.append((blk, whole, (value * pct) - whole))
    blks_info = sorted(blks_info, key=lambda info: info[2], reverse=True)
    for i in range(0, value - sum_wholes):
        blks_info[i] = (blks_info[i][0], blks_info[i][1] + 1, blks_info[i][2])

    amount_distr = 0
    for tuple in blks_info:
        blk_key_map[tuple[0]][key] = tuple[1]
        amount_distr += tuple[1]
    if amount_distr < value:
        print("Distribution lacking:", prec, key, (value - amount_distr), sep=" ")

def make_final_blk_map(log, prec_blk_key_map):
    log.dprint("Build block to fields map (disaggregate)")
    print("Build block to fields map (disaggregate)")
    props_total = {}
    final_blk_map = {}              # {blkid: {prop1: val1, prop2: val2, ...}, ...}
    for srprec, blks in prec_blk_key_map.items():
        for block, key_map in blks.items():
            if not (block in final_blk_map):
                final_blk_map[block] = {}
            for key, value in key_map.items():
                if not (key in final_blk_map[block]):
                    final_blk_map[block][key] = 0
                final_blk_map[block][key] += value
                sum_props(props_total, key, value)

    pp = log.pretty_printer()
    log.dprint("Props totals")
    pp.pprint(props_total)        

    return final_blk_map