# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Filed by Candidate)

## Property Leads Database

## Overview  
This repository contains the schema and DDL scripts for a normalized property‑leads database. The model captures leads, property details, valuations, rehab estimates, taxes, HOA fees, and various lookup/config tables.

## Schema Diagram  



## Normalization & Design Decisions  
- **1NF (Atomicity)**  
  - All repeating metrics (e.g. list price, Zestimate, ARV, rent estimates) moved into the `valuation` table keyed by `valuation_id` rather than as separate columns in `property`.  
  - Rehab flags (paint, flooring, foundation, etc.) remain in `rehab` but could be further broken into a config table if requirements grow.  
- **2NF & 3NF (Eliminate Redundancy)**  
  - Lookup tables for all descriptive domains (source, selling_reason, reviewer, state, city, flood_zone, property_type, parking_type, layout_type, subdivision, market) to avoid typos and ensure consistency.  
  - M:N mapping tables (e.g. `hoa` → `hoa_lookup`) separate values/flags from property associations.  
- **Assumptions**  
  - Each property has at most one `taxes` record (enforced via unique FK on `property_id`).  
  - Valuations and rehabs can have multiple records per property to track historical changes or multiple vendor quotes.  
  - Lookup tables use auto‑increment surrogate keys for simplicity.

## Tables & Lookups  
| Table                   | Description                                       |
|-------------------------|---------------------------------------------------|
| `source_lookup`         | Valid lead sources (Internal, Auction.com, MLS…)  |
| `selling_reason_lookup` | Why a property is listed (Downsizing, Investor…)  |
| `final_reviewer_lookup` | User who reviewed the lead                        |
| `leads`                 | Marketing leads metadata                          |
| `state_lookup`          | US state codes                                    |
| `city_lookup`           | Cities, linked to `state_lookup`                  |
| `address`               | Street, city, ZIP                                 |
| `market_lookup`         | Market regions (Chicago, Tampa, Dallas…)          |
| `flood_lookup`          | Flood zone designations                           |
| `property_type_lookup`  | SFR, Duplex, Townhouse, etc.                      |
| `parking_type_lookup`   | Garage, Street, Driveway…                         |
| `layout_type_lookup`    | Ranch, Colonial, Split…                           |
| `subdivision_lookup`    | Subdivision names                                 |
| `property`              | Core property metadata                            |
| `hoa_lookup`            | HOA fee + flag combinations                       |
| `hoa`                   | M:N mapping from `property` to HOA entries        |
| `taxes`                 | Single tax record per property                    |
| `valuation`             | Key‐value metrics per property valuation          |
| `rehab`                 | Rehab cost estimates + feature flags              |

## Running & Testing

1. **Connect to MySQL**  
   Ensure you have a running MySQL instance.

2. **Run the DDL script to create all tables**  
   ```bash
   mysql -u db_user -p home_db < tables.sql
3. **Verify tables exist**
   ```
   SHOW TABLES;
   DESCRIBE property;
**Document your ETL logic here:**

- Outline your approach and design
- Provide instructions and code snippets for running the ETL
- List any requirements
