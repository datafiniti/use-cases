import java.util.HashMap;
import java.util.Map;

public class AddressAbbreviator {

    // Abbreviations dictionary
    private static final Map<String, String> abbreviations = new HashMap<>();

    static {
        // Street suffixes
        abbreviations.put("ALLEY", "ALY");
        abbreviations.put("ANNEX", "ANX");
        abbreviations.put("ARCADE", "ARC");
        abbreviations.put("AVENUE", "AVE");
        abbreviations.put("BAYOO", "BYU");
        abbreviations.put("BEACH", "BCH");
        abbreviations.put("BEND", "BND");
        abbreviations.put("BLUFF", "BLF");
        abbreviations.put("BLUFFS", "LFS");
        abbreviations.put("BOTTOM", "BTM");
        abbreviations.put("BOULEVARD", "BLVD");
        abbreviations.put("BRANCH", "BR");
        abbreviations.put("BRIDGE", "BRG");
        abbreviations.put("BROOK", "BRK");
        abbreviations.put("BROOKS", "BRKS");
        abbreviations.put("BURG", "BG");
        abbreviations.put("BURGS", "BGS");
        abbreviations.put("BYPASS", "BYP");
        abbreviations.put("CAMP", "CP");
        abbreviations.put("CANYON", "CYN");
        abbreviations.put("CAPE", "CPE");
        abbreviations.put("CAUSEWAY", "CSWY");
        abbreviations.put("CENTER", "CTR");
        abbreviations.put("CENTERS", "CTRS");
        abbreviations.put("CIRCLE", "CIR");
        abbreviations.put("CIRCLES", "CIRS");
        abbreviations.put("CLIFF", "CLF");
        abbreviations.put("CLIFFS", "CLFS");
        abbreviations.put("CLUB", "CLB");
        abbreviations.put("COMMON", "CMN");
        abbreviations.put("CORNER", "COR");
        abbreviations.put("CORNERS", "CORS");
        abbreviations.put("COURSE", "CRSE");
        abbreviations.put("COURT", "CT");
        abbreviations.put("COURTS", "CTS");
        abbreviations.put("COVE", "CV");
        abbreviations.put("COVES", "CVS");
        abbreviations.put("CREEK", "CRK");
        abbreviations.put("CRESCENT", "CRES");
        abbreviations.put("CREST", "CRST");
        abbreviations.put("CROSSING", "XING");
        abbreviations.put("CROSSROAD", "XRD");
        abbreviations.put("CURVE", "CURV");
        abbreviations.put("DALE", "DL");
        abbreviations.put("DAM", "DM");
        abbreviations.put("DIVIDE", "DV");
        abbreviations.put("DRIVE", "DR");
        abbreviations.put("DRIVES", "DRS");
        abbreviations.put("ESTATE", "EST");
        abbreviations.put("ESTATES", "ESTS");
        abbreviations.put("EXPRESSWAY", "EXPY");
        abbreviations.put("EXTENSION", "EXT");
        abbreviations.put("EXTENSIONS", "EXTS");
        abbreviations.put("FALL", "FALL");
        abbreviations.put("FALLS", "FLS");
        abbreviations.put("FERRY", "FRY");
        abbreviations.put("FIELD", "FLD");
        abbreviations.put("FIELDS", "FLDS");
        abbreviations.put("FLAT", "FLT");
        abbreviations.put("FLATS", "FLTS");
        abbreviations.put("FORD", "FRD");
        abbreviations.put("FORDS", "FRDS");
        abbreviations.put("FOREST", "FRST");
        abbreviations.put("FORGE", "FRG");
        abbreviations.put("FORGES", "FRGS");
        abbreviations.put("FORK", "FRK");
        abbreviations.put("FORKS", "FRKS");
        abbreviations.put("FORT", "FT");
        abbreviations.put("FREEWAY", "FWY");
        abbreviations.put("GARDEN", "GDN");
        abbreviations.put("GARDENS", "GDNS");
        abbreviations.put("GATEWAY", "GTWY");
        abbreviations.put("GLEN", "GLN");
        abbreviations.put("GLENS", "GLNS");
        abbreviations.put("GREEN", "GRN");
        abbreviations.put("GREENS", "GRNS");
        abbreviations.put("GROVE", "GRV");
        abbreviations.put("GROVES", "GRVS");
        abbreviations.put("HARBOR", "HBR");
        abbreviations.put("HARBORS", "HBRS");
        abbreviations.put("HAVEN", "HVN");
        abbreviations.put("HEIGHTS", "HTS");
        abbreviations.put("HIGHWAY", "HWY");
        abbreviations.put("HILL", "HL");
        abbreviations.put("HILLS", "HLS");
        abbreviations.put("HOLLOW", "HOLW");
        abbreviations.put("INLET", "INLT");
        abbreviations.put("INTERSTATE", "I");
        abbreviations.put("ISLAND", "IS");
        abbreviations.put("ISLANDS", "ISS");
        abbreviations.put("ISLE", "ISLE");
        abbreviations.put("JUNCTION", "JCT");
        abbreviations.put("JUNCTIONS", "JCTS");
        abbreviations.put("KEY", "KY");
        abbreviations.put("KEYS", "KYS");
        abbreviations.put("KNOLL", "KNL");
        abbreviations.put("KNOLLS", "KNLS");
        abbreviations.put("LAKE", "LK");
        abbreviations.put("LAKES", "LKS");
        abbreviations.put("LAND", "LAND");
        abbreviations.put("LANDING", "LNDG");
        abbreviations.put("LANE", "LN");
        abbreviations.put("LIGHT", "LGT");
        abbreviations.put("LIGHTS", "LGTS");
        abbreviations.put("LOAF", "LF");
        abbreviations.put("LOCK", "LCK");
        abbreviations.put("LOCKS", "LCKS");
        abbreviations.put("LODGE", "LDG");
        abbreviations.put("LOOP", "LOOP");
        abbreviations.put("MALL", "MALL");
        abbreviations.put("MANOR", "MNR");
        abbreviations.put("MANORS", "MNRS");
        abbreviations.put("MEADOW", "MDW");
        abbreviations.put("MEADOWS", "MDWS");
        abbreviations.put("MEWS", "MEWS");
        abbreviations.put("MILL", "ML");
        abbreviations.put("MILLS", "MLS");
        abbreviations.put("MISSION", "MSN");
        abbreviations.put("MOORHEAD", "MHD");
        abbreviations.put("MOTORWAY", "MTWY");
        abbreviations.put("MOUNT", "MT");
        abbreviations.put("MOUNTAIN", "MTN");
        abbreviations.put("MOUNTAINS", "MTNS");
        abbreviations.put("NECK", "NCK");
        abbreviations.put("ORCHARD", "ORCH");
        abbreviations.put("OVAL", "OVAL");
        abbreviations.put("OVERPASS", "OPAS");
        abbreviations.put("PARK", "PARK");
        abbreviations.put("PARKS", "PARK");
        abbreviations.put("PARKWAY", "PKWY");
        abbreviations.put("PARKWAYS", "PKWY");
        abbreviations.put("PASS", "PASS");
        abbreviations.put("PASSAGE", "PSGE");
        abbreviations.put("PATH", "PATH");
        abbreviations.put("PIKE", "PIKE");
        abbreviations.put("PINE", "PNE");
        abbreviations.put("PINES", "PNES");
        abbreviations.put("PLACE", "PL");
        abbreviations.put("PLAIN", "PLN");
        abbreviations.put("PLAINS", "PLNS");
        abbreviations.put("PLAZA", "PLZ");
        abbreviations.put("POINT", "PT");
        abbreviations.put("POINTS", "PTS");
        abbreviations.put("PORT", "PRT");
        abbreviations.put("PORTS", "PRTS");
        abbreviations.put("PRAIRIE", "PR");
        abbreviations.put("RADIAL", "RADL");
        abbreviations.put("RAMP", "RAMP");
        abbreviations.put("RANCH", "RNCH");
        abbreviations.put("RAPID", "RPD");
        abbreviations.put("RAPIDS", "RPDS");
        abbreviations.put("REST", "RST");
        abbreviations.put("RIDGE", "RDG");
        abbreviations.put("RIDGES", "RDGS");
        abbreviations.put("RIVER", "RIV");
        abbreviations.put("ROAD", "RD");
        abbreviations.put("ROADS", "RDS");
        abbreviations.put("ROUTE", "RTE");
        abbreviations.put("ROW", "ROW");
        abbreviations.put("RUE", "RUE");
        abbreviations.put("RUN", "RUN");
        abbreviations.put("SHOAL", "SHL");
        abbreviations.put("SHOALS", "SHLS");
        abbreviations.put("SHORE", "SHR");
        abbreviations.put("SHORES", "SHRS");
        abbreviations.put("SKYWAY", "SKWY");
        abbreviations.put("SPRING", "SPG");
        abbreviations.put("SPRINGS", "SPGS");
        abbreviations.put("SPUR", "SPUR");
        abbreviations.put("SPURS", "SPUR");
        abbreviations.put("SQUARE", "SQ");
        abbreviations.put("SQUARES", "SQS");
        abbreviations.put("STATION", "STA");
        abbreviations.put("STREAM", "STRM");
        abbreviations.put("STREET", "ST");
        abbreviations.put("STREETS", "STS");
        abbreviations.put("SUMMIT", "SMT");
        abbreviations.put("THROUGHWAY", "TRWY");
        abbreviations.put("TRACE", "TRCE");
        abbreviations.put("TRACK", "TRAK");
        abbreviations.put("TRAIL", "TRL");
        abbreviations.put("TUNNEL", "TUNL");
        abbreviations.put("TURNPIKE", "TPKE");
        abbreviations.put("UNDERPASS", "UPAS");
        abbreviations.put("UNION", "UN");
        abbreviations.put("UNIONS", "UNS");
        abbreviations.put("VALLEY", "VLY");
        abbreviations.put("VALLEYS", "VLYS");
        abbreviations.put("VIADUCT", "VIA");
        abbreviations.put("VIEW", "VW");
        abbreviations.put("VIEWS", "VWS");
        abbreviations.put("VILLAGE", "VLG");
        abbreviations.put("VILLAGES", "VLGS");
        abbreviations.put("VILLE", "VL");
        abbreviations.put("VISTA", "VIS");
        abbreviations.put("WALK", "WALK");
        abbreviations.put("WALKS", "WALK");
        abbreviations.put("WALL", "WALL");
        abbreviations.put("WAY", "WAY");
        abbreviations.put("WAYS", "WAYS");
        abbreviations.put("WELL", "WL");
        abbreviations.put("WELLS", "WLS");
        abbreviations.put("APARTMENT", "APT");
        abbreviations.put("BASEMENT", "BSMT");
        abbreviations.put("BUILDING", "BLDG");
        abbreviations.put("DEPARTMENT", "DEPT");
        abbreviations.put("FLOOR", "FL");
        abbreviations.put("FRONT", "FRNT");
        abbreviations.put("HANGAR", "HNGR");
        abbreviations.put("LOBBY", "LBBY");
        abbreviations.put("LOT", "LOT");
        abbreviations.put("LOWER", "LOWR");
        abbreviations.put("OFFICE", "OFC");
        abbreviations.put("PENTHOUSE", "PH");
        abbreviations.put("PIER", "PIER");
        abbreviations.put("REAR", "REAR");
        abbreviations.put("ROOM", "RM");
        abbreviations.put("SIDE", "SIDE");
        abbreviations.put("SLIP", "SLIP");
        abbreviations.put("SPACE", "SPC");
        abbreviations.put("STOP", "STOP");
        abbreviations.put("SUITE", "STE");
        abbreviations.put("TRAILER", "TRLR");
        abbreviations.put("UNIT", "UNIT");
        abbreviations.put("UPPER", "UPPR");

        // Directions
        abbreviations.put("NORTH", "N");
        abbreviations.put("SOUTH", "S");
        abbreviations.put("EAST", "E");
        abbreviations.put("WEST", "W");
        abbreviations.put("NORTHWEST", "NW");
        abbreviations.put("NORTHEAST", "NE");
        abbreviations.put("SOUTHEAST", "SE");
        abbreviations.put("SOUTHWEST", "SW");

        // Provinces, states, and directions
        abbreviations.put("ALBERTA", "AB");
        abbreviations.put("BRITISH COLUMBIA", "BC");
        abbreviations.put("MANITOBA", "MB");
        abbreviations.put("NEW BRUNSWICK", "NB");
        abbreviations.put("NEWFOUNDLAND AND LABRADOR", "NL");
        abbreviations.put("NORTHWEST TERRITORIES", "NT");
        abbreviations.put("NOVA SCOTIA", "NS");
        abbreviations.put("NUNAVUT", "NU");
        abbreviations.put("ONTARIO", "ON");
        abbreviations.put("PRINCE EDWARD ISLAND", "PE");
        abbreviations.put("QUEBEC", "QC");
        abbreviations.put("SASKATCHEWAN", "SK");
        abbreviations.put("YUKON", "YT");
        abbreviations.put("ALABAMA", "AL");
        abbreviations.put("ALASKA", "AK");
        abbreviations.put("ARIZONA", "AZ");
        abbreviations.put("ARKANSAS", "AR");
        abbreviations.put("CALIFORNIA", "CA");
        abbreviations.put("COLORADO", "CO");
        abbreviations.put("CONNECTICUT", "CT");
        abbreviations.put("DELAWARE", "DE");
        abbreviations.put("FLORIDA", "FL");
        abbreviations.put("GEORGIA", "GA");
        abbreviations.put("HAWAII", "HI");
        abbreviations.put("IDAHO", "ID");
        abbreviations.put("ILLINOIS", "IL");
        abbreviations.put("INDIANA", "IN");
        abbreviations.put("IOWA", "IA");
        abbreviations.put("KANSAS", "KS");
        abbreviations.put("KENTUCKY", "KY");
        abbreviations.put("LOUISIANA", "LA");
        abbreviations.put("MAINE", "ME");
        abbreviations.put("MARYLAND", "MD");
        abbreviations.put("MASSACHUSETTS", "MA");
        abbreviations.put("MICHIGAN", "MI");
        abbreviations.put("MINNESOTA", "MN");
        abbreviations.put("MISSISSIPPI", "MS");
        abbreviations.put("MISSOURI", "MO");
        abbreviations.put("MONTANA", "MT");
        abbreviations.put("NEBRASKA", "NE");
        abbreviations.put("NEVADA", "NV");
        abbreviations.put("NEW HAMPSHIRE", "NH");
        abbreviations.put("NEW JERSEY", "NJ");
        abbreviations.put("NEW MEXICO", "NM");
        abbreviations.put("NEW YORK", "NY");
        abbreviations.put("NORTH CAROLINA", "NC");
        abbreviations.put("NORTH DAKOTA", "ND");
        abbreviations.put("OHIO", "OH");
        abbreviations.put("OKLAHOMA", "OK");
        abbreviations.put("OREGON", "OR");
        abbreviations.put("PENNSYLVANIA", "PA");
        abbreviations.put("RHODE ISLAND", "RI");
        abbreviations.put("SOUTH CAROLINA", "SC");
        abbreviations.put("SOUTH DAKOTA", "SD");
        abbreviations.put("TENNESSEE", "TN");
        abbreviations.put("TEXAS", "TX");
        abbreviations.put("UTAH", "UT");
        abbreviations.put("VERMONT", "VT");
        abbreviations.put("VIRGINIA", "VA");
        abbreviations.put("WASHINGTON", "WA");
        abbreviations.put("WEST VIRGINIA", "WV");
        abbreviations.put("WISCONSIN", "WI");
        abbreviations.put("WYOMING", "WY");
    }

    public static String abbreviateAddress(String address) {
        StringBuilder result = new StringBuilder();

        // Split the address into parts (preserve commas)
        String[] parts = address.split("(?=[,])|\\s+");

        for (String part : parts) {
            if (part.trim().isEmpty()) {
                continue;
            }
            // Remove special characters for lookup, then match against abbreviations
            String sanitized = part.replaceAll("[^a-zA-Z0-9]", "").toUpperCase();
            String abbreviated = abbreviations.getOrDefault(sanitized, part);

            // Append the abbreviated part with any trailing comma or space
            result.append(abbreviated);
            if (part.endsWith(",")) {
                result.append(",");
            }
            result.append(" ");
        }

        // Trim and return the final result
        return result.toString().trim();
    }

    public static void main(String[] args) {
        // Example input
        String userAddress = "123, Main Street, Alberta, Northwest";
        System.out.println("Original Address: " + userAddress);
        System.out.println("Abbreviated Address: " + abbreviateAddress(userAddress));
    }
}
