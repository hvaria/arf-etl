/**
 * src/etl-config.ts
 *
 * Single source of truth for all CCLD facility ETL configuration.
 * Edit this file to change sources, fields, filters, or enrichment behaviour.
 */

export interface EtlConfig {
  sources: {
    geo: { enabled: boolean; resourceId: string; pageSize: number };
    ccl: { enabled: boolean; resourceId: string; pageSize: number };
  };
  fieldMap: {
    fromGeo: {
      number: string; name: string; lat: string; lng: string;
      address: string; city: string; zip: string; phone: string;
      capacity: string; status: string; typeCode: string;
    };
    fromCcl: {
      number: string; name: string; facilityType: string; licensee: string;
      administrator: string; status: string; capacity: string;
      firstLicenseDate: string; closedDate: string; county: string;
    };
  };
  filterByGroups:    string[];
  filterByCounties:  string[];
  skipMissingGeo:    boolean;
  includeCclOnly:    boolean;
  dryRun:            boolean;
  limit:             number;
  enrichment: {
    enabled:           boolean;
    fields: {
      lastInspectionDate: boolean;
      administrator:      boolean;
      licensee:           boolean;
      totalTypeB:         boolean;
      citations:          boolean;
    };
    requestsPerSecond: number;
    skipIfPopulated:   boolean;
    enrichLimit:       number;
    enrichCounties:    readonly string[];
  };
}

export const ETL_CONFIG = {
  sources: {
    geo: {
      enabled:    true,
      resourceId: "f9c77b0d-9711-4f34-8c7f-90f542fbc24a",
      pageSize:   5000,
    },
    ccl: {
      enabled:    true,
      resourceId: "9f5d1d00-6b24-4f44-a158-9cbe4b43f117",
      pageSize:   5000,
    },
  },

  fieldMap: {
    fromGeo: {
      number:   "FAC_NBR",
      name:     "NAME",
      lat:      "FAC_LATITUDE",
      lng:      "FAC_LONGITUDE",
      address:  "RES_STREET_ADDR",
      city:     "RES_CITY",
      zip:      "RES_ZIP_CODE",
      phone:    "FAC_PHONE_NBR",
      capacity: "CAPACITY",
      status:   "STATUS",
      typeCode: "TYPE",
    },
    fromCcl: {
      number:          "facility_number",
      name:            "facility_name",
      facilityType:    "facility_type",
      licensee:        "licensee",
      administrator:   "facility_administrator",
      status:          "facility_status",
      capacity:        "facility_capacity",
      firstLicenseDate: "license_first_date",
      closedDate:      "closed_date",
      county:          "county",
    },
  },

  // Set to [] to include all groups, or list specific groups to filter:
  //   "Adult & Senior Care" | "Child Care" | "Children's Residential" | "Home Care"
  filterByGroups: [] as string[],

  // Set to [] to include all counties, or list specific counties to filter:
  //   e.g. ["Sacramento", "Los Angeles", "San Diego"]
  filterByCounties: [] as string[],

  // Skip GEO rows that have no usable coordinates
  skipMissingGeo: true,

  // Include CCL-only records (no matching GEO row → no lat/lng)
  includeCclOnly: true,

  // When true: parse and log but do not write to the database
  dryRun: false,

  // Maximum records to process (0 = no limit; use for smoke tests)
  limit: 0,

  enrichment: {
    enabled: true,
    fields: {
      lastInspectionDate: true,
      administrator:      true,
      licensee:           true,
      totalTypeB:         true,
      citations:          true,
    },
    // CCLD Transparency API calls per second (be polite to the server)
    requestsPerSecond: 5,
    // Skip a field if it already has a value from the CHHS extraction
    skipIfPopulated: true,
    // Max facilities to enrich per ETL run (0 = all)
    enrichLimit: 0,
    // Only enrich facilities in these counties ([] = all counties)
    enrichCounties: [] as readonly string[],
  },
} satisfies EtlConfig;
