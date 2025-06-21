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

# tracking
track_counties = False

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

def sum_props(prop_totals_map, prop_key, prop_value, countymap=None, countyfp=None, countyname=None):
    try:
        intval = int(prop_value)
        if not (prop_key in prop_totals_map):
            prop_totals_map[prop_key] = 0
        prop_totals_map[prop_key] += intval

        if countymap != None and countyfp != None:
            if not (countyfp in countymap):
                countymap[countyfp] = {"FP": countyfp, "Name": countyname if countyname != None else ''}
            if not (prop_key in countymap[countyfp]):
                countymap[countyfp][prop_key] = 0
            countymap[countyfp][prop_key] += intval

    except:
        ignore = 0
    return

"""
    The following set of functions exists to filter the fields in 2022 election datasets.
    The data we have obtained often has all of the state legislative races, which are numerous and we are not using,
    so it simply slows this process considerably
"""
# TODO This really needs state and year (5/3/23)
def cong_party_wa(source_year, cand_code):
    consufx = cand_code[4:9]
    if source_year == 2022:
        if (consufx == "01DEL" or consufx == "02LAR" or consufx == "03PER" or consufx == "04WHI" or consufx == "05HIL" or 
            consufx == "06KIL" or consufx == "07JAY" or consufx == "08SCH" or consufx == "09SMI" or consufx == "10STR"):
            return "DVAR"
        if (consufx == "01CAV" or consufx == "02MAT" or consufx == "03KEN" or consufx == "04NEW" or consufx == "05ROD" or 
            consufx == "06KRE" or consufx == "07MOO" or consufx == "08LAR" or consufx == "09BAS" or consufx == "10SWA"):
            return "RVAR"
    elif source_year == 2024:
        if (cand_code[4:6] == "04" or cand_code[4:6] == "09"):      # Two R's in 4 and two D's in 9; don't report any results
            return None
        if (consufx == "01DEL" or consufx == "02LAR" or consufx == "03PER" or consufx == "05CON" or 
            consufx == "06RAN" or consufx == "07JAY" or consufx == "08GOW" or consufx == "10STR"):  # Date mislabelled Schrier
            return "DVAR"
        if (consufx == "01BRE" or consufx == "02HAR" or consufx == "03KEN" or consufx == "05BAU" or 
            consufx == "06MAC" or consufx == "07ALE" or consufx == "08GOE" or consufx == "10HEW"):
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

def party_code(party):
    return "DVAR" if party == "D" else "RVAR" if party == "R" else "IOTH"

def ohio_supreme_court(year, cand_code):
    if year == 2022:
        if cand_code[7:10] == "FIS" or cand_code[7:10] == "JAM":
            return "SC1"
        else:
            return "SC2"
    elif year == 2024:
        if cand_code[7:10] == "DET" or cand_code[7:10] == "STE":
            return "SC1"
        elif cand_code[7:10] == "SHA" or cand_code[7:10] == "DON":
            return "SC2"
        else:           # FOR, HAW
            return "SC3"
    return "UNK"

def filter_prop_key(cand_code, state, source_year):
    if source_year == 2008:
        contest = None
        if cand_code == "PresD":
            contest = cand_code
        elif cand_code == "PresR":
            contest = cand_code
        elif cand_code == "PresTot":
            contest = cand_code          # Total not other, handle in Convert
        return contest
    elif source_year == 2014 and state == "MA":
        contest = None
        suffix = cand_code[5:6]
        match cand_code[0:5]:
            case "SEN14": 
                contest = "SEN14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
            case "GOV14":
                contest = "GOV14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
        return contest
    elif source_year == 2014 and state == "TX":
        contest = None
        suffix = cand_code[5:6]
        match cand_code[0:5]:
            case "SEN14": 
                contest = "SEN14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
            case "GOV14":
                contest = "GOV14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
        return contest
    elif source_year == 2014 and state == "MN":
        contest = None
        suffix = cand_code[5:6]
        if cand_code[0:4] == "AG14":
            contest = "AG14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
        else:
            match cand_code[0:5]:
                case "SEN14": 
                    contest = "SEN14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
                case "GOV14":
                    contest = "GOV14" + ("DXXX" if suffix == "D" else "RXXX" if suffix == "R" else "IOTH")
        return contest
    elif source_year == 2014 and state == "NC":
        contest = None
        suffix = cand_code[5:6]
        if cand_code[0:8] == "EL14G_US":
            contest = cand_code
        return contest
    elif state == "DC":
        contest = None
        party = cand_code[6:7]
        prefix = cand_code[0:1]

        if prefix == "G" or prefix == "R" or prefix == "S":     # Only General, Runoff or Special, not Primary
            # Congress
            year = cand_code[1:3]
            contest_code = cand_code[3:6]
            match contest_code:
                case "PRE":
                    contest = prefix + year + "PRE" + party_code(party)
                case "MAY":  # Mayor, use GOV
                    contest = prefix + year + "GOV" + party_code(party)
                case "USS":  # Shadow Senator, only for 2020 since we already included it
                    contest = (prefix + year + "USS" + party_code(party)) if source_year == 2020 else None
                case "ATG":
                    contest = prefix + year + "ATG" + party_code(party)
                case "DEL":  # Delegate to Congress
                    contest = prefix + year + "CON" + party_code(party)
                    
        return contest
    elif source_year <= 2020:
        return cand_code
    elif source_year == 2024 and (state != "WA" and state != "AL" and state != "SC" and state != "LA" and state != "OH" and state != "AZ"):       # Add states handled more fully as they are obtained
        # NYT data
        contest = None
        if cand_code == "votes_dem":
            contest = "G24PREDHAR"
        elif cand_code == "votes_rep":
            contest = "G24PRERTRU"
        elif cand_code == "votes_total":
            contest = "G24PREIOTH"
        return contest
    elif state == "WA":
        contest = None
        if source_year == 2022:
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
        elif source_year == 2024:
            suffix = cand_code[6:]
            match cand_code[3:6]:
                case "SEN": 
                    contest = cand_code[0:3] + "SEN" + ("DCAN" if suffix.startswith("CANT") else "RGAR" if suffix.startswith("GARC") else "IOTH")
                case "SOS":
                    contest = cand_code[0:3] + "SOS" + ("DHOB" if suffix.startswith("HOBB") else "RWHI" if suffix.startswith("WHIT") else "IOTH")
                case "PRS":
                    contest = cand_code[0:3] + "PRE" + ("DHAR" if suffix.startswith("HARR") else "RTRU" if suffix.startswith("TRUM") else "IOTH")
                case "GOV":
                    contest = cand_code[0:3] + "GOV" + ("DFER" if suffix.startswith("FERG") else "RREI" if suffix.startswith("REIC") else "IOTH")
                case "LTG":
                    contest = cand_code[0:3] + "LTG" + ("DHEC" if suffix.startswith("HECK") else "RMAT" if suffix.startswith("MATT") else "IOTH")
                case "TRE":
                    contest = cand_code[0:3] + "TRE" + ("DPEL" if suffix.startswith("PELL") else "RHAN" if suffix.startswith("HANE") else "IOTH")
                case "AUD":
                    contest = cand_code[0:3] + "AUD" + ("DMCC" if suffix.startswith("MCCA") else "RHAW" if suffix.startswith("HAWK") else "IOTH")
                case "ATG":
                    contest = cand_code[0:3] + "ATG" + ("DBRO" if suffix.startswith("BROW") else "RSER" if suffix.startswith("SERR") else "IOTH")
                case "CPL":
                    contest = cand_code[0:3] + "PLC" + ("DUPT" if suffix.startswith("UPTH") else "RBEU" if suffix.startswith("BEUT") else "IOTH")
                case _:
                    if cand_code[3:5] == "C0" or cand_code[3:5] == "C1":
                        party_suffix = cong_party_wa(source_year, cand_code)
                        if party_suffix != None:
                            contest = cand_code[0:3] + "CON" + party_suffix
        return contest
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
    elif (state == "WI" or state == "TX" or state == "LA" or state == "HI" or state == "OH" or state == "AL" or state == "MT" or state == "GA" or state == "FL" or
          state == "SC" or state == "IL" or state == "MS" or state == "NM" or state == "NY" or state == "AZ" or state == "NV" or state == "KS" or state == "TN" or
          state == "OK"):
        # Mostly RDH States
        contest = None
        party = cand_code[6:7]
        prefix = cand_code[0:1]

        if prefix == "G" or prefix == "R" or prefix == "S":     # Only General, Runoff or Special, not Primary
            # Congress
            if cand_code[1:4] == "CON":
                contest = prefix + (str(source_year)[2:4]) + "CON" + party_code(party)
            else:
                year = cand_code[1:3]
                contest_code = cand_code[3:6]
                match contest_code:
                    case "PRE":
                        contest = prefix + year + "PRE" + party_code(party)
                    case "GOV":
                        contest = prefix + year + "GOV" + party_code(party)
                    case "USS":
                        contest = prefix + year + "USS" + party_code(party)
                    case "SOS":
                        contest = prefix + year + "SOS" + party_code(party)
                    case "ATG":
                        contest = prefix + year + "ATG" + party_code(party)
                    case "LTG":
                        contest = prefix + year + "LTG" + party_code(party)
                    case "TRE":
                        contest = prefix + year + "TRE" + party_code(party)
                    case "COM":
                        contest = prefix + year + "CMP" + party_code(party)
                    case "CFO":
                        contest = prefix + year + "TRE" + party_code(party)
                    case "AUD":
                        contest = prefix + year + "AUD" + party_code(party)
                    
                    # Supreme Court varies by state
                    case "JUS":
                        if state == "OH":
                            contest = prefix + year + ohio_supreme_court(source_year, cand_code) + party_code(party)
                        elif state == "NM" and (party == "D" or party == "R"):
                            contest = prefix + year + "SC1" + party_code(party)
                    case "JS2":
                        if state == "NM":
                            contest = prefix + year + "SC2" + party_code(party)
                    case "CJU":
                        if state == "OH":
                            contest = prefix + year + "SCC" + party_code(party)
                    case "AJ5":
                        if state == "AL":
                            contest = prefix + year + "SC5" + party_code(party)
                    case "AJ6":
                        if state == "AL":
                            contest = prefix + year + "SC6" + party_code(party)

        return contest
    elif state == "CA" and source_year == 2022:
        contest = None
        year = str(source_year)[2:4]
        prefix = "G"
        sameparty = True if cand_code[6:8] == "02" else False
        party = "IOTH" if sameparty else "DVAR" if cand_code[3:6] == "DEM" else "RVAR" if cand_code[3:6] == "REP" else "IOTH"       # Use other for 2nd candidate in same party
        match cand_code[0:3]:
            case "USS": 
                contest = f'{prefix}{year}SEN{party}'
            case "GOV":
                contest = f'{prefix}{year}GOV{party}'
            case "LTG":
                contest = f'{prefix}{year}LTG{party}'
            case "TRS":
                contest = f'{prefix}{year}TRE{party}'
            case "SOS":
                contest = f'{prefix}{year}SOS{party}'
            case "ATG":
                contest = f'{prefix}{year}ATG{party}'
            #case "CNG":                                    # In CA 2022, 6 congressional contests were Dem vs Dem, so we're not going to add the data
            #    contest = f'{prefix}{year}CON{party}'
        return contest
    elif state == "MN":
        contest = None
        match cand_code[0:2]:
            case "MN":
                match cand_code[2:4]:
                    case "GO":
                        if cand_code[5] != "T":
                            contest = "G22" + "GOV" + party_code(cand_code[5])
                    case "SO":
                        if cand_code[5] != "T":
                            contest = "G22" + "SOS" + party_code(cand_code[5])
                    case "AU":
                        if cand_code[5] != "T":
                            contest = "G22" + "AUD" + party_code(cand_code[5])
                    case "AG":
                        if cand_code[4] != "T":
                            contest = "G22" + "ATG" + party_code(cand_code[4])
            case "US":
                if cand_code[2:5] == "REP" and cand_code[5] != "T":
                    contest = "G22" + "CON" + party_code(cand_code[5])
        return contest
    elif state == "AK":
        contest = None
        party = cand_code[6:7]
        match cand_code[0:6]:
            case "G22GOV":
                contest = "G22" + "GOV" + party_code(party)
            case "G22USS":
                contest = "G22" + "USS" + party_code(party)
            case "G22CON":
                contest = "G22" + "CON" + party_code(party)
        return contest
    elif state == "PR" and source_year == 2016:
        contest = None
        match cand_code[0:4]:
            case "GOV1":
                contest = "G16" + "GOV" + ("DVAR" if cand_code == "GOV16PPD" else "RVAR" if cand_code == "GOV16PNP" else "IOTH")
            case "RC16":
                contest = "G16" + "CON" + ("DVAR" if cand_code == "RC16PPD" else "RVAR" if cand_code == "RC16PNP" else "IOTH")
        return contest

    """                 # Code for original file from BJR; superseded by RDH file
    elif state == "NY":
        contest = None
        if len(cand_code) > 3 and cand_code[0:4] == "Gov_":
            party = cand_code[4:]
            contest = "G22" + "GOV" + ("DVAR" if (party == "DEM" or party == "WOR") else "RVAR" if (party == "REP" or party == "CON") else "IOTH")
        elif len(cand_code) > 5 and cand_code[0:6] == "USSen_":
            party = cand_code[6:]
            contest = "G22" + "USS" + ("DVAR" if (party == "DEM" or party == "WOR") else "RVAR" if (party == "REP" or party == "CON") else "IOTH")
        elif len(cand_code) > 4 and cand_code[0:5] == "Comp_":
            party = cand_code[5:]
            contest = "G22" + "CMP" + ("DVAR" if (party == "DEM" or party == "WOR") else "RVAR" if (party == "REP" or party == "CON") else "IOTH")
        elif len(cand_code) > 2 and cand_code[0:3] == "AG_":
            party = cand_code[3:]
            contest = "G22" + "ATG" + ("DVAR" if (party == "DEM" or party == "WOR") else "RVAR" if (party == "REP" or party == "CON") else "IOTH")
        elif len(cand_code) > 7 and cand_code[0:8] == "USHouse_":
            party = cand_code[8:]
            contest = "G22" + "CON" + ("DVAR" if (party == "DEM" or party == "WOR") else "RVAR" if (party == "REP" or party == "CON") else "IOTH")
        return contest
    """

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
    """

def make_block_props_map(log, source_props_path, block_map_path, block_pop_map, source_key, use_index_for_source_key, ok_to_agg, state, source_year, listpropsonly, sourceIsCsv):
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

        source_props_total = {}
        source_props_cnty_total = {}
        # Build source props map {srckey1: {prop1: val1, ...}, ...}
        source_props_map = {}
        if (sourceIsCsv):
            with open(source_props_path) as source_csv_file:
                source_rows = csv.DictReader(source_csv_file, delimiter=",")
                for row in tqdm(source_rows):
                    srprec = ""
                    if source_key in row:
                        srprec = row[source_key]
                        source_props_map[srprec] = {}
                        for prop_key, prop_value in row.items():
                            if prop_key != source_key and ok_to_agg(prop_key, prop_value):
                                source_props_map[srprec][prop_key] = prop_value
                                sum_props(source_props_total, prop_key, prop_value)
                    else:
                        print("Blkprops csv: No key", end="\n")
                        continue
        else:
            source_props = gpd.read_file(source_props_path)
            for i in source_props.index:
                source_props_item = source_props.loc[i]
                srckey = i if use_index_for_source_key else str(source_props_item[source_key])
                source_props_map[srckey] = {}
                countyfp = source_props_item["COUNTYFP"] if track_counties and ("COUNTYFP" in source_props_item) else None
                countyname = source_props_item["CNTY_NAME"] if track_counties and ("CNTY_NAME" in source_props_item) else None
                for prop_key, prop_value in source_props_item.items():
                    if prop_key != srckey and ok_to_agg(prop_key, prop_value):
                        # To handle more contests, call
                        prop_key = filter_prop_key(prop_key, state, source_year)
                        if prop_key != None:
                            if not (prop_key in source_props_map[srckey]):
                                source_props_map[srckey][prop_key] = 0
                            source_props_map[srckey][prop_key] += int(prop_value)
                            sum_props(source_props_total, prop_key, prop_value, countymap=source_props_cnty_total, countyfp=countyfp, countyname=countyname)
                    
        # Hook to move props from 1 srckey to another, in rare cases
        adjust_source_props_map(state, source_year, source_props_map)

        pp = log.pretty_printer()
        log.dprint("Source Props totals")
        pp.pprint(source_props_total)
        if track_counties:
            pp.pprint(source_props_cnty_total)
        if listpropsonly:
            return None

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

#def keep_key(key):
#    return key[0:3] == "ATG" or key[0:3] == "GOV" or key[0:3] == "USS" or key[0:3] == "LTG" or key[0:3] == "PRS"

def make_block_props_map_ca(log, source_props_path, block_map_path, ok_to_agg, source_year, listpropsonly):
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
        source_props_total = {}
        source_props_cnty_total = {}
        for row in tqdm(source_rows):
            srprec = ""
            if "SRPREC_KEY" in row:
                srprec = row["SRPREC_KEY"]
            elif "COUNTY" in row:
                if row["COUNTY"] == "CNTYTOT":
                    continue
                srprec = srprec_key(row["COUNTY"], row["SRPREC"])
                print("Using COUNTY")
            if srprec != "":
                source_props_map[srprec] = {"SRPREC_KEY": srprec}
                countyfp = row["COUNTY"] if track_counties and ("COUNTY" in row) else None
                countyname = row["CNTY_NAME"] if track_counties and ("CNTY_NAME" in row) else None
                for prop_key, prop_value in row.items():
                    prop_key = filter_prop_key(prop_key, "CA", source_year)
                    if prop_key != None and ok_to_agg(prop_key, prop_value):
                        source_props_map[srprec][prop_key] = prop_value
                        sum_props(source_props_total, prop_key, prop_value, countymap=source_props_cnty_total, countyfp=countyfp, countyname=countyname)

        pp = log.pretty_printer()
        log.dprint("Source Props totals")
        pp.pprint(source_props_total)        
        if track_counties:
            pp.pprint(source_props_cnty_total)
        if listpropsonly:
            return None

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
        prec_blk_key_map = make_prec_blk_key_map(log, prec_blk_pct_map, source_props_map, ok_to_agg)
        return make_final_blk_map(log, prec_blk_key_map)

def make_prec_blk_key_map(log, prec_blk_pct_map, source_props_map, ok_to_agg):
    prec_blk_key_map = {}
    cant_disagg_set = {}

    all_precs = set({})
    for srprec in source_props_map.keys():
        all_precs.add(srprec)

    props_total = {}
    props_cnty_total = {}
    left_out_props_total = {}
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
                        countyfp = blk[2:5] if track_counties else None
                        sum_props(props_total, key, value, countymap=props_cnty_total, countyfp=countyfp)
                    except:
                        if not (key in cant_disagg_set):
                            print("Can't disagg key: ", key)
                            cant_disagg_set[key] = True

        else:
            log.dprint("Prec Key not found: ", srprec)

    pp = log.pretty_printer()
    log.dprint("Props totals from Distributing")
    pp.pprint(props_total)
    if track_counties:
        pp.pprint(props_cnty_total)

    left_out_precs = all_precs - seen_precs
    if len(left_out_precs):
        log.dprint(f'Left out precs: {left_out_precs}')
        log.dprint("Props totals left out from Distributing")
        ppc = log.pretty_printer_compact()
        for srprec in left_out_precs:
            has_nonzero = False
            for key, value in source_props_map[srprec].items():
                if ok_to_agg(key):
                    has_nonzero = has_nonzero or (int(value) != 0)
                    sum_props(left_out_props_total, key, value)
            if has_nonzero:
                ppc.pprint(source_props_map[srprec])
        pp.pprint(left_out_props_total)

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
    props_cnty_total = {}
    final_blk_map = {}              # {blkid: {prop1: val1, prop2: val2, ...}, ...}
    for srprec, blks in prec_blk_key_map.items():
        for block, key_map in blks.items():
            if not (block in final_blk_map):
                final_blk_map[block] = {}
            for key, value in key_map.items():
                if not (key in final_blk_map[block]):
                    final_blk_map[block][key] = 0
                final_blk_map[block][key] += value      # accumulate in case a block receives values from > 1 precinct
                countyfp = block[2:5] if track_counties else None
                sum_props(props_total, key, value, countymap=props_cnty_total, countyfp=countyfp)

    pp = log.pretty_printer()
    log.dprint("Props totals")
    pp.pprint(props_total)
    if track_counties:
        pp.pprint(props_cnty_total)

    return final_blk_map

def adjust_source_props_map(state, source_year, source_props_map):
    if state == "WA" and source_year == 2024:
        # One precinct that they dump extra votes in, where actual precinct is not determined
        if "KI00008888" in source_props_map and "KI00000990" in source_props_map:
            row = source_props_map["KI00008888"]
            for key, value in row.items():
                source_props_map["KI00000990"][key] += value
                source_props_map["KI00008888"][key] = 0


