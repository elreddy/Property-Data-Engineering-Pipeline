-- LEADS:

-- 1. Lookup table: Source
CREATE TABLE source_lookup (
  source_id   INT AUTO_INCREMENT PRIMARY KEY,
  source_name VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 2. Lookup table: Selling Reason
CREATE TABLE selling_reason_lookup (
  selling_reason_id INT AUTO_INCREMENT PRIMARY KEY,
  selling_reason    VARCHAR(255) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 3. Lookup table: Reviewer
CREATE TABLE final_reviewer_lookup (
  reviewer_id   INT AUTO_INCREMENT PRIMARY KEY,
  reviewer_name VARCHAR(255) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- Leads table
CREATE TABLE leads (
  lead_id                  INT AUTO_INCREMENT PRIMARY KEY,
  property_title           VARCHAR(225) NOT NULL UNIQUE,
  reviewed_status          VARCHAR(50)     ,
  most_recent_status       VARCHAR(50)     ,
  source_id                INT             ,
  occupancy                VARCHAR(50),
  net_yield                DECIMAL(6,2),
  irr                      DECIMAL(6,2),
  selling_reason_id        INT,
  seller_retained_broker   VARCHAR(255),
  reviewer_id              INT,
  CONSTRAINT fk_leads_source
    FOREIGN KEY (source_id) REFERENCES source_lookup(source_id),
  CONSTRAINT fk_leads_selling_reason
    FOREIGN KEY (selling_reason_id) REFERENCES selling_reason_lookup(selling_reason_id),
  CONSTRAINT fk_leads_reviewer
    FOREIGN KEY (reviewer_id) REFERENCES final_reviewer_lookup(reviewer_id)
) ENGINE=InnoDB;

-- PROPERTY:

-- 1. State lookup
CREATE TABLE state_lookup (
  state_id    INT      AUTO_INCREMENT PRIMARY KEY,
  state_code  CHAR(2)  NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 2. City lookup
CREATE TABLE city_lookup (
  city_id    INT            AUTO_INCREMENT PRIMARY KEY,
  city_name  VARCHAR(100)   NOT NULL,
  state_id   INT            NOT NULL,
  CONSTRAINT fk_city_state
    FOREIGN KEY (state_id) REFERENCES state_lookup(state_id)
) ENGINE=InnoDB;

-- 3. Address table
CREATE TABLE address (
  address_id      INT            AUTO_INCREMENT PRIMARY KEY,
  street_address  VARCHAR(255)   NOT NULL,
  city_id         INT            NOT NULL,
  zip             INT            NOT NULL,
  CONSTRAINT fk_address_city
    FOREIGN KEY (city_id) REFERENCES city_lookup(city_id)
) ENGINE=InnoDB;

-- 4. Flood zone lookup
CREATE TABLE flood_lookup (
  flood_id    INT            AUTO_INCREMENT PRIMARY KEY,
  flood_zone  VARCHAR(50)    NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 5. Property type lookup
CREATE TABLE property_type_lookup (
  type_id    INT            AUTO_INCREMENT PRIMARY KEY,
  type_name  VARCHAR(50)    NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 6. Parking type lookup
CREATE TABLE parking_type_lookup (
  parking_id    INT            AUTO_INCREMENT PRIMARY KEY,
  parking_desc  VARCHAR(50)    NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 7. Layout type lookup
CREATE TABLE layout_type_lookup (
  layout_id    INT            AUTO_INCREMENT PRIMARY KEY,
  layout_desc  VARCHAR(50)    NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 8. Subdivision lookup
CREATE TABLE subdivision_lookup (
  subdivision_id   INT            AUTO_INCREMENT PRIMARY KEY,
  subdivision_name VARCHAR(100)   NOT NULL UNIQUE
) ENGINE=InnoDB;

-- 9. Market lookup table
CREATE TABLE market_lookup (
  market_id    INT            AUTO_INCREMENT PRIMARY KEY,
  market_name  VARCHAR(100)   NOT NULL UNIQUE
) ENGINE=InnoDB;

-- Property table
CREATE TABLE property (
  property_id         INT               AUTO_INCREMENT PRIMARY KEY,
  property_title      VARCHAR(255)      NOT NULL,
  address_id          INT               NOT NULL,
  lead_id             INT               NOT NULL,
  market_id           INT               NULL,
  flood_id            INT               NULL,
  type_id             INT               NULL,
  highway             VARCHAR(50)       NULL,
  train               VARCHAR(50)       NULL,
  tax_rate            DECIMAL(5,2)      NULL,
  sqft_basement       INT               NULL,
  htw                 VARCHAR(10)       NULL,
  pool                VARCHAR(10)       NULL,
  commercial          VARCHAR(10)       NULL,
  water               VARCHAR(50)       NULL,
  sewage              VARCHAR(50)       NULL,
  year_built          SMALLINT          NULL,
  sqft_mu             INT               NULL,
  sqft_total          INT               NULL,
  parking_id          INT               NULL,
  bed                 TINYINT           NULL,
  bath                TINYINT           NULL,
  basementyesno       VARCHAR(10)       NULL,
  layout_id           INT               NULL,
  rent_restricted     VARCHAR(10)       NULL,
  neighborhood_rating TINYINT           NULL,
  latitude            DECIMAL(9,6)      NULL,
  longitude           DECIMAL(9,6)      NULL,
  subdivision_id      INT               NULL,
  school_average      DECIMAL(3,2)      NULL,
  CONSTRAINT fk_property_address   FOREIGN KEY (address_id)     REFERENCES address(address_id),
  CONSTRAINT fk_property_market    FOREIGN KEY (market_id)      REFERENCES market_lookup(market_id),
  CONSTRAINT fk_property_flood     FOREIGN KEY (flood_id)       REFERENCES flood_lookup(flood_id),
  CONSTRAINT fk_property_lead      FOREIGN KEY (lead_id)        REFERENCES leads(lead_id),
  CONSTRAINT fk_property_type      FOREIGN KEY (type_id)        REFERENCES property_type_lookup(type_id),
  CONSTRAINT fk_property_parking   FOREIGN KEY (parking_id)     REFERENCES parking_type_lookup(parking_id),
  CONSTRAINT fk_property_layout    FOREIGN KEY (layout_id)      REFERENCES layout_type_lookup(layout_id),
  CONSTRAINT fk_property_subdivision FOREIGN KEY (subdivision_id) REFERENCES subdivision_lookup(subdivision_id)
) ENGINE=InnoDB;

-- HOA:

-- 1. HOA lookup table
CREATE TABLE hoa_lookup (
  hoa_lookup_id INT AUTO_INCREMENT PRIMARY KEY,
  hoa_value     INT NOT NULL,
  hoa_flag      VARCHAR(10) NOT NULL,
  UNIQUE (hoa_value, hoa_flag)
) ENGINE=InnoDB;

-- 2. HOA mapping table
CREATE TABLE hoa (
  hoa_id         INT AUTO_INCREMENT PRIMARY KEY,
  property_id    INT NOT NULL,
  hoa_lookup_id  INT NOT NULL,
  CONSTRAINT fk_hoa_property FOREIGN KEY (property_id)   REFERENCES property(property_id)  ON DELETE CASCADE,
  CONSTRAINT fk_hoa_lookup   FOREIGN KEY (hoa_lookup_id) REFERENCES hoa_lookup(hoa_lookup_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- TAXES:
-- 1. taxes table
CREATE TABLE taxes (
  tax_id      INT AUTO_INCREMENT PRIMARY KEY,
  property_id INT NOT NULL,
  tax_value   DECIMAL(10,2),
  CONSTRAINT fk_taxes_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- VALUATION:
CREATE TABLE valuation (
  valuation_id     INT AUTO_INCREMENT PRIMARY KEY,
  property_id      INT NOT NULL,
  previous_rent    DECIMAL(12,2),
  list_price       DECIMAL(12,2),
  zestimate        DECIMAL(12,2),
  arv              DECIMAL(12,2),
  expected_rent    DECIMAL(12,2),
  rent_zestimate   DECIMAL(12,2),
  low_fmr          DECIMAL(12,2),
  high_fmr         DECIMAL(12,2),
  redfin_value     DECIMAL(12,2),
  CONSTRAINT fk_valuation_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- REHAB:
CREATE TABLE rehab (
  rehab_id           INT AUTO_INCREMENT PRIMARY KEY,
  property_id        INT NOT NULL,
  underwriting_rehab DECIMAL(12,2),
  rehab_calculation  DECIMAL(12,2),
  paint              VARCHAR(10),
  flooring_flag      VARCHAR(10),
  foundation_flag    VARCHAR(10),
  roof_flag          VARCHAR(10),
  hvac_flag          VARCHAR(10),
  kitchen_flag       VARCHAR(10),
  bathroom_flag      VARCHAR(10),
  appliances_flag    VARCHAR(10),
  windows_flag       VARCHAR(10),
  landscaping_flag   VARCHAR(10),
  trashout_flag      VARCHAR(10),
  CONSTRAINT fk_rehab_property FOREIGN KEY (property_id) REFERENCES property(property_id) ON DELETE CASCADE
) ENGINE=InnoDB;
