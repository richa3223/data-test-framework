@legacy
Feature: Kpler Oil: curated to certified ship to ship

  # THIS PIPELINE IS NO LONGER AVAILABLE IN DATABRICKS - 5 Sep 2023

  Background:
    Given the testdata folder: "oil/curated_to_certified"
    And the following databases have been flagged for cleanup
      | database                    |
      | geography                   |
      | kpler_curated_oil           |
      | maritime                    |
      | master_ref_data             |

   Scenario: Kpler Oil: curated to certified ship to ship
    Given I pre-create the following databases at the following locations
      | database | container | path                |
      | maritime | certified | relational/maritime |
    And I create the input tables for scenario "ship_to_ship/01_simple_data_transformation"
    When I run the curated_to_certified_ship_to_ship_oil task from the pipeline glencore-synergy-pipelines-kpler_kpler_main
    Then the pipeline result is "SUCCESS"
    When I download the following tables
      | table                             | db_location_container   | db_location            | format |
      | maritime.ship_to_ship             | certified               | relational/maritime/_db| delta  |
      | maritime.ship_to_ship_repair_zone | certified               | relational/maritime/_db| delta  |
    Then the downloaded "maritime.ship_to_ship_repair_zone" table should contain the "ship_to_ship/01_simple_data_transformation" output data
      | ignore_columns                  |
      | JOB_ID                          |
      | PROCESSING_TS                   | 
      | UPDATED_AT                      |
      | UPDATED_BY                      |
      | TIMESTAMP                       |
      | TARGET_SURROGATE_KEY            |
      | SURROGATE_KEY                   |
    And the values in the "SURROGATE_KEY" column of the downloaded "maritime.ship_to_ship_repair_zone" tables should be unique
    Then the downloaded "maritime.ship_to_ship" table should contain the "ship_to_ship/01_simple_data_transformation" output data
      | ignore_columns            |
      | _year                     |
      | _metadata.has_warnings    |
      | _metadata.pipeline_run_id |
      | _metadata.pipeline_run_ts |
      | _metadata.surrogate_key   |
      | updated_on                |
    Given I append to the input tables for scenario "ship_to_ship/02_new_rows_get_appended"
    When I run the curated_to_certified_ship_to_ship_oil task from the pipeline glencore-synergy-pipelines-kpler_kpler_main
    Then the pipeline result is "SUCCESS"
    When I download the following tables
      | table                             | db_location_container   | db_location            | format |
      | maritime.ship_to_ship             | certified               | relational/maritime/_db| delta   |
      | maritime.ship_to_ship_repair_zone | certified               | relational/maritime/_db| delta  |
    Then the downloaded "maritime.ship_to_ship_repair_zone" table should contain the "ship_to_ship/02_new_rows_get_appended" output data
      | ignore_columns                  |
      | JOB_ID                          |
      | PROCESSING_TS                   | 
      | UPDATED_AT                      |
      | UPDATED_BY                      |
      | TIMESTAMP                       |
      | TARGET_SURROGATE_KEY            |
      | SURROGATE_KEY                   |
    And the values in the "SURROGATE_KEY" column of the downloaded "maritime.ship_to_ship_repair_zone" tables should be unique
    Then the downloaded "maritime.ship_to_ship" table should contain the "ship_to_ship/02_new_rows_get_appended" output data
      | ignore_columns            |
      | _year                     |
      | _metadata.has_warnings    |
      | _metadata.pipeline_run_id |
      | _metadata.pipeline_run_ts |
      | _metadata.surrogate_key   |
      | updated_on                |
    And the values in the "_metadata.surrogate_key" column of the downloaded "maritime.ship_to_ship" tables should be unique
