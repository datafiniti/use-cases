/**
 * Datafiniti Product Matching Config
 * ----------------------------------
 * This configuration object defines how incoming product data
 * is transformed into a Datafiniti query for product matching.
 *
 * Primary match strategy:
 *   1. Try brand + manufacturerNumber (strong match)
 *   2. Fallback to GTIN match
 *
 * Used in product enrichment / deduplication workflows.
 */

const raw = {

  // -----------------------------
  // API CONFIG
  // -----------------------------
  dataType: 'products', // Datafiniti endpoint to query

  token: 'YOUR_API_TOKEN_HERE', // ⚠️ API token (should be moved to env var in production)

  forcedAnd: false, // Do NOT force all query clauses into AND (we build custom logic below)

  // -----------------------------
  // QUERY CONFIGURATION
  // -----------------------------
  query: {

    /**
     * Maps incoming input fields → Datafiniti query field names
     * This allows flexible source schemas.
     */
    fieldsMapping: {
      gtins: "gtins",
      brand: 'brand',
      manufacturerNumber: 'manufacturerNumber',
      manufacturer: 'manufacturer'
    },

    /**
     * Pre-process each field BEFORE it becomes part of the query.
     * Used for quoting, normalization, or formatting.
     */
    fieldsPreProcessing: {

      // Ensure GTIN is treated as a string
      gtins: gtins => `${gtins}`,

      // Brand as raw string
      brand: brand => `${brand}`,

      // Manufacturer wrapped in quotes for exact match
      manufacturer: manufacturer => `"${manufacturer}"`,

      // Manufacturer number wrapped in quotes for exact match
      manufacturerNumber: manufacturerNumber => `"${manufacturerNumber}"`
    },

    /**
     * Custom query builder.
     * Receives pre-processed fields and returns a full Datafiniti query string.
     */
    advanceQueryManipulation: (processedFields) => {

      let { gtins, brand, manufacturerNumber, manufacturer } = processedFields

      // Remove field prefixes added by upstream logic (e.g., "brand:")
      gtins = gtins.replace('gtins:', '')
      brand = brand.replace('brand:', '')
      manufacturerNumber = manufacturerNumber.replace('manufacturerNumber:', '')
      manufacturer = manufacturer.replace('manufacturer:', '')

      // Primary structured match + GTIN fallback
      return `((brand:"${brand}" OR manufacturer:${manufacturer}) AND manufacturerNumber:${manufacturerNumber}) OR gtins:"${gtins}"`

      /**
       * Alternate GTIN-only strategy (disabled):
       * Uncomment to force strict GTIN matching.
       */
    },

    // Limit number of candidate matches returned per query
    numRecords: 5
  },

  // -----------------------------
  // RESULT MAPPING CONFIG
  // -----------------------------
  mapping: {
    key: 'gtins' // Use GTIN as the unique match key in downstream processing
  },

  // -----------------------------
  // DATA CLEANUP / OUTPUT CONTROL
  // -----------------------------

  unwantedData: [], // Fields to exclude from results (none currently)

  flatFields: [], // Fields to flatten from nested → top-level (none configured)

  forcedFields: [], // Fields to always include even if null (none configured)

  // -----------------------------
  // CUSTOM FIELD PARSERS
  // -----------------------------
  customParsers: {
    // Add per-field transformation logic here if needed
  }
}

// Export config for use in matching pipeline
module.exports = raw