import re

# Full original abbreviations
abbreviations = {
    "ALLEY": "Aly", "ANNEX": "Anx", "ARCADE": "Arc", "AVENUE": "Ave",
    "BAYOO": "Byu", "BEACH": "Bch", "BEND": "Bnd", "BLUFF": "Blf",
    "BLUFFS": "Blfs", "BOTTOM": "Btm", "BOULEVARD": "Blvd", "BRANCH": "Br",
    "BRIDGE": "Brg", "BROOK": "Brk", "BROOKS": "Brks", "BURG": "Bg",
    "BURGS": "Bgs", "BYPASS": "Byp", "CAMP": "Cp", "CANYON": "Cyn",
    "CAPE": "Cpe", "CAUSEWAY": "Cswy", "CENTER": "Ctr", "CENTERS": "Ctrs",
    "CIRCLE": "Cir", "CIRCLES": "Cirs", "CLIFF": "Clf", "CLIFFS": "Clfs",
    "CLUB": "Clb", "COMMON": "Cmn", "CORNER": "Cor", "CORNERS": "Cors",
    "COURSE": "Crse", "COURT": "Ct", "COURTS": "Cts", "COVE": "Cv",
    "COVES": "Cvs", "CREEK": "Crk", "CRESCENT": "Cres", "CREST": "Crst",
    "CROSSING": "Xing", "CROSSROAD": "Xrd", "CURVE": "Curv", "DALE": "Dl",
    "DAM": "Dm", "DIVIDE": "Dv", "DRIVE": "Dr", "DRIVES": "Drs",
    "ESTATE": "Est", "ESTATES": "Ests", "EXPRESSWAY": "Expy", "EXTENSION": "Ext",
    "EXTENSIONS": "Exts", "FALL": "Fall", "FALLS": "Fls", "FERRY": "Fry",
    "FIELD": "Fld", "FIELDS": "Flds", "FLAT": "Flt", "FLATS": "Flts",
    "FORD": "Frd", "FORDS": "Frds", "FOREST": "Frst", "FORGE": "Frg",
    "FORGES": "Frgs", "FORK": "Frk", "FORKS": "Frks", "FORT": "Ft",
    "FREEWAY": "Fwy", "GARDEN": "Gdn", "GARDENS": "Gdns", "GATEWAY": "Gtwy",
    "GLEN": "Gln", "GLENS": "Glns", "GREEN": "Grn", "GREENS": "Grns",
    "GROVE": "Grv", "GROVES": "Grvs", "HARBOR": "Hbr", "HARBORS": "Hbrs",
    "HAVEN": "Hvn", "HEIGHTS": "Hts", "HIGHWAY": "Hwy", "HILL": "Hl",
    "HILLS": "Hls", "HOLLOW": "Holw", "INLET": "Inlt", "INTERSTATE": "I",
    "ISLAND": "Is", "ISLANDS": "Iss", "ISLE": "Isle", "JUNCTION": "Jct",
    "JUNCTIONS": "Jcts", "KEY": "Ky", "KEYS": "Kys", "KNOLL": "Knl",
    "KNOLLS": "Knls", "LAKE": "Lk", "LAKES": "Lks", "LAND": "Land",
    "LANDING": "Lndg", "LANE": "Ln", "LIGHT": "Lgt", "LIGHTS": "Lgts",
    "LOAF": "Lf", "LOCK": "Lck", "LOCKS": "Lcks", "LODGE": "Ldg",
    "LOOP": "Loop", "MALL": "Mall", "MANOR": "Mnr", "MANORS": "Mnrs",
    "MEADOW": "Mdw", "MEADOWS": "Mdws", "MEWS": "Mews", "MILL": "Ml",
    "MILLS": "Mls", "MISSION": "Msn", "MOTORWAY": "Mtw y", "MOUNT": "Mt",
    "MOUNTAIN": "Mtn", "MOUNTAINS": "Mtns", "NECK": "Nck", "ORCHARD": "Orch",
    "OVAL": "Oval", "OVERPASS": "Opas", "PARK": "Park", "PARKS": "Park",
    "PARKWAY": "Pkwy", "PARKWAYS": "Pkwy", "PASS": "Pass", "PASSAGE": "Psge",
    "PATH": "Path", "PIKE": "Pike", "PINE": "Pne", "PINES": "Pnes",
    "PLACE": "Pl", "PLAIN": "Pln", "PLAINS": "Plns", "PLAZA": "Plz",
    "POINT": "Pt", "POINTS": "Pts", "PORT": "Prt", "PORTS": "Prts",
    "PRAIRIE": "Pr", "RADIAL": "Radl", "RAMP": "Ramp", "RANCH": "Rnch",
    "RAPID": "Rpd", "RAPIDS": "Rpds", "REST": "Rst", "RIDGE": "Rdg",
    "RIDGES": "Rdgs", "RIVER": "Riv", "ROAD": "Rd", "ROADS": "Rds",
    "ROUTE": "Rte", "ROW": "Row", "RUE": "Rue", "RUN": "Run",
    "SHOAL": "Shl", "SHOALS": "Shls", "SHORE": "Shr", "SHORES": "Shrs",
    "SKYWAY": "Skywy", "SPRING": "Spg", "SPRINGS": "Spgs", "SPUR": "Spur",
    "SPURS": "Spur", "SQUARE": "Sq", "SQUARES": "Sqs", "STATION": "Sta",
    "STREAM": "Strm", "STREET": "St", "STREETS": "Sts", "SUMMIT": "Smt",
    "TRACE": "Trce", "TRACK": "Trak", "TRAIL": "Trl", "TUNNEL": "Tunl",
    "TURNPIKE": "Tpke", "UNDERPASS": "Upas", "UNION": "Un", "UNIONS": "Uns",
    "VALLEY": "Vly", "VALLEYS": "Vlys", "VIADUCT": "Via", "VIEW": "Vw",
    "VIEWS": "Vws", "VILLAGE": "Vlg", "VILLAGES": "Vlgs", "VILLE": "Vl",
    "VISTA": "Vis", "WALK": "Walk", "WALKS": "Walk", "WALL": "Wall",
    "WAY": "Way", "WAYS": "Ways", "WELL": "Wl", "WELLS": "Wls"
}

# Directions
directions = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHWEST": "NW", "NORTHEAST": "NE",
    "SOUTHEAST": "SE", "SOUTHWEST": "SW"
}

def normalize_address(address: str) -> str:
    # Clean extra spaces
    address = re.sub(r"\s+", " ", address.strip())
    words = address.split()
    
    # Uppercase check for UNIT
    unit_indices = [i for i, w in enumerate(words) if w.upper() == "UNIT"]
    
    # Determine which street type to abbreviate
    street_positions = [i for i, w in enumerate(words) if w.upper() in abbreviations]
    target_index = None
    
    if street_positions:
        if unit_indices:
            # Abbreviate the street type immediately left of the first UNIT
            for idx in reversed(street_positions):
                if idx < unit_indices[0]:
                    target_index = idx
                    break
        else:
            # No UNIT, abbreviate last street type
            target_index = street_positions[-1]
    
    # Process words
    for i, word in enumerate(words):
        uw = word.upper()
        
        if uw == "UNIT":
            words[i] = "UNIT"
            continue
        
        if uw in directions:
            words[i] = directions[uw]
            continue
        
        if target_index is not None and i == target_index:
            words[i] = abbreviations[uw]
            continue
        
        words[i] = word.capitalize()
    
    return " ".join(words)

# --- User input ---
if __name__ == "__main__":
    user_input = input("Enter an address to normalize: ")
    print("Normalized:", normalize_address(user_input))