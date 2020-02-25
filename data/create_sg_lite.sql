-- Created on 2020-02-12

-- Questions:
--

-- Notes:
-- 1. Only five datatypes supported: Null, Integer, Real, Text, Blob
---   Convert booleans to integer, uuids and datetime to text
-- 2. Spatialite requires an integer primary key and ROWID for geometry
--    columns to work properly... will try first with BLOBs instead.
--    See example in db_setup.R for examples of converting to and from
--    hex and binary.
-- 3. Design notes: https://www.gaia-gis.it/gaia-sins/spatialite-cookbook-5/cookbook_topics.03.html#topic_Creating_a_well_designed_DB
-- 4. Sometimes get error on first run when using spatialite gui...just delete and recreate db once again...then run and success.

-- ToDo:
-- 1. Load sqlite with WRIA 22 and 23 data. Nick has not loaded anything since July of 2019.

-- Spatial examples:
--SELECT AddGeometryColumn('fish_biologist_district', 'geom', 2927, 'POLYGON', 'XY');
--SELECT CreateSpatialIndex("geonames", "Geometry");

-- Current date 2020-02-12

-- Enable spatialite extension so invokation of CreateUUID() function does not throw error
-- Not needed if using spatialite gui
-- SELECT load_extension('mod_spatialite')

-- Create tables: Location Level -----------------------------------------------------------------------------------------------------

-- Location ------------------------------------------------------

CREATE TABLE waterbody_lut (
    waterbody_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    waterbody_name text NOT NULL,
    waterbody_display_name text,
    latitude_longitude_id character varying(13),
    stream_catalog_code text,
    tributary_to_name text,
    obsolete_flag integer NOT NULL DEFAULT false,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE stream (
    stream_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    waterbody_id text NOT NULL,
    geom BLOB NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_waterbody_lut__stream FOREIGN KEY (waterbody_id) REFERENCES waterbody_lut (waterbody_id)
) WITHOUT ROWID;

CREATE TABLE wria_lut (
    wria_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    wria_code character varying(2) NOT NULL,
    wria_description text NOT NULL,
    geom BLOB NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE location_type_lut (
    location_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    location_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE location_orientation_type_lut (
    location_orientation_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    orientation_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE stream_channel_type_lut (
    stream_channel_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    channel_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE location (
    location_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    waterbody_id text NOT NULL,
    wria_id text NOT NULL,
    location_type_id text NOT NULL,
    stream_channel_type_id text NOT NULL,
    location_orientation_type_id text NOT NULL,
    river_mile_measure decimal(6,2),
    location_code text,
    location_name text,
    location_description text,
    waloc_id integer,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_waterbody_lut__location FOREIGN KEY (waterbody_id) REFERENCES waterbody_lut(waterbody_id),
    CONSTRAINT fk_wria_lut__location FOREIGN KEY (wria_id) REFERENCES wria_lut(wria_id),
    CONSTRAINT fk_location_type_lut__location FOREIGN KEY (location_type_id) REFERENCES location_type_lut(location_type_id),
    CONSTRAINT fk_stream_channel_type_lut__location FOREIGN KEY (stream_channel_type_id) REFERENCES stream_channel_type_lut(stream_channel_type_id),
    CONSTRAINT fk_location_orientation_type_lut__location FOREIGN KEY (location_orientation_type_id) REFERENCES location_orientation_type_lut(location_orientation_type_id)
) WITHOUT ROWID;

CREATE TABLE location_coordinates (
    location_coordinates_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    location_id text NOT NULL,
    horizontal_accuracy decimal(8,2),
    comment_text text,
    geom BLOB NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_location__location_coordinates FOREIGN KEY (location_id) REFERENCES location(location_id)
) WITHOUT ROWID;

CREATE TABLE media_type_lut (
    media_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    media_type_code text NOT NULL,
    media_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE media_location (
    media_location_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    location_id text NOT NULL,
    media_type_id text NOT NULL,
    media_url text NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_location__media_location FOREIGN KEY (location_id) REFERENCES location(location_id),
    CONSTRAINT fk_media_type_lut__media_location FOREIGN KEY (media_type_id) REFERENCES media_type_lut(media_type_id)
) WITHOUT ROWID;

CREATE TABLE species_lut (
    species_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    species_code text NOT NULL,
    common_name text NOT NULL,
    genus text,
    species text,
    sub_species text,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE run_lut (
    run_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    run_short_description text NOT NULL,
    run_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE stock_lut (
    stock_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    waterbody_id text NOT NULL,
    species_id text NOT NULL,
    run_id text NOT NULL,
    sasi_stock_number text,
    stock_name text,
    status_code text,
    esa_code text,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text,
    CONSTRAINT fk_waterbody_lut__stock_lut FOREIGN KEY (waterbody_id) REFERENCES waterbody_lut(waterbody_id),
    CONSTRAINT fk_species_lut__stock_lut FOREIGN KEY (species_id) REFERENCES species_lut(species_id),
    CONSTRAINT fk_run_lut__stock_lut FOREIGN KEY (run_id) REFERENCES run_lut(run_id)
) WITHOUT ROWID;

-- Create tables: Survey Level ------------------------------------------------------------------------------------------------

--  Survey ------------------------------------------------------

CREATE TABLE data_source_lut (
    data_source_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    data_source_name text NOT NULL,
    data_source_code text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE data_source_unit_lut (
    data_source_unit_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    data_source_unit_name text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_method_lut (
    survey_method_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_method_code text NOT NULL,
    survey_method_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE data_review_status_lut (
    data_review_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    data_review_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_completion_status_lut (
    survey_completion_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    completion_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE incomplete_survey_type_lut (
    incomplete_survey_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    incomplete_survey_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey (
    survey_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_datetime text NOT NULL,
    data_source_id text NOT NULL,
    data_source_unit_id text NOT NULL,
    survey_method_id text NOT NULL,
    data_review_status_id text NOT NULL,
    upper_end_point_id text NOT NULL,
    lower_end_point_id text NOT NULL,
    survey_completion_status_id text,
    incomplete_survey_type_id text NOT NULL,
    survey_start_datetime text,
    survey_end_datetime text,
    observer_last_name text,
    data_submitter_last_name text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_data_source_lut__survey FOREIGN KEY (data_source_id) REFERENCES data_source_lut(data_source_id),
    CONSTRAINT fk_data_source_unit_lut__survey FOREIGN KEY (data_source_unit_id) REFERENCES data_source_unit_lut(data_source_unit_id),
    CONSTRAINT fk_survey_method_lut__survey FOREIGN KEY (survey_method_id) REFERENCES survey_method_lut(survey_method_id),
    CONSTRAINT fk_data_review_status_lut__survey FOREIGN KEY (data_review_status_id) REFERENCES data_review_status_lut(data_review_status_id),
    CONSTRAINT fk_location__survey__upper_end_point FOREIGN KEY (upper_end_point_id) REFERENCES location(location_id),
    CONSTRAINT fk_location__survey__lower_end_point FOREIGN KEY (lower_end_point_id) REFERENCES location(location_id),
    CONSTRAINT fk_survey_completion_status_lut__survey FOREIGN KEY (survey_completion_status_id) REFERENCES survey_completion_status_lut(survey_completion_status_id),
    CONSTRAINT fk_incomplete_survey_type_lut__survey FOREIGN KEY (incomplete_survey_type_id) REFERENCES incomplete_survey_type_lut(incomplete_survey_type_id)
) WITHOUT ROWID;

-- Survey comment ------------------------------------------------------

CREATE TABLE area_surveyed_lut (
    area_surveyed_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    area_surveyed text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_abundance_condition_lut (
    fish_abundance_condition_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_abundance_condition text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE stream_condition_lut (
    stream_condition_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    stream_condition text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE stream_flow_type_lut (
    stream_flow_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    flow_type_short_description text NOT NULL,
    flow_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_count_condition_lut (
    survey_count_condition_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_count_condition text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_direction_lut (
    survey_direction_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_direction_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_timing_lut (
    survey_timing_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_timing text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE visibility_condition_lut (
    visibility_condition_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    visibility_condition text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE visibility_type_lut (
    visibility_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    visibility_type_short_description text NOT NULL,
    visibility_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE weather_type_lut (
    weather_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    weather_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_comment (
    survey_comment_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    area_surveyed_id text,
    fish_abundance_condition_id text,
    stream_condition_id text,
    stream_flow_type_id text,
    survey_count_condition_id text,
    survey_direction_id text,
    survey_timing_id text,
    visibility_condition_id text,
    visibility_type_id text,
    weather_type_id text,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__survey_comment FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_area_surveyed_lut__survey_comment FOREIGN KEY (area_surveyed_id) REFERENCES area_surveyed_lut(area_surveyed_id),
    CONSTRAINT fk_fish_abundance_condition_lut__survey_comment FOREIGN KEY (fish_abundance_condition_id) REFERENCES fish_abundance_condition_lut(fish_abundance_condition_id),
    CONSTRAINT fk_survey_count_condition_lut__survey_comment FOREIGN KEY (survey_count_condition_id) REFERENCES survey_count_condition_lut(survey_count_condition_id),
    CONSTRAINT fk_survey_direction_lut__survey_comment FOREIGN KEY (survey_direction_id) REFERENCES survey_direction_lut(survey_direction_id),
    CONSTRAINT fk_survey_timing_lut__survey_comment FOREIGN KEY (survey_timing_id) REFERENCES survey_timing_lut(survey_timing_id),
    CONSTRAINT fk_stream_condition_lut__survey_comment FOREIGN KEY (stream_condition_id) REFERENCES stream_condition_lut(stream_condition_id),
    CONSTRAINT fk_weather_type_lut__survey_comment FOREIGN KEY (weather_type_id) REFERENCES weather_type_lut(weather_type_id),
    CONSTRAINT fk_visibility_condition_lut__survey_comment FOREIGN KEY (visibility_condition_id) REFERENCES visibility_condition_lut(visibility_condition_id),
    CONSTRAINT fk_stream_flow_type_lut__survey_comment FOREIGN KEY (stream_flow_type_id) REFERENCES stream_flow_type_lut(stream_flow_type_id),
    CONSTRAINT fk_visibility_type_lut__survey_comment FOREIGN KEY (visibility_type_id) REFERENCES visibility_type_lut(visibility_type_id)
) WITHOUT ROWID;

-- Survey intent ------------------------------------------------------

CREATE TABLE count_type_lut (
    count_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    count_type_code text NOT NULL,
    count_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_intent (
    survey_intent_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    species_id text NOT NULL,
    count_type_id text NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__survey_intent FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_species_lut__survey_intent FOREIGN KEY (species_id) REFERENCES species_lut(species_id),
    CONSTRAINT fk_count_type_lut__survey_intent FOREIGN KEY (count_type_id) REFERENCES count_type_lut(count_type_id)
) WITHOUT ROWID;

-- Mobile survey form ------------------------------------------------------

CREATE TABLE mobile_survey_form (
    mobile_survey_form_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    parent_form_survey_id integer NOT NULL,
    parent_form_survey_guid text NOT NULL,
    parent_form_name text NOT NULL,
    parent_form_id text NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__mobile_survey_form FOREIGN KEY (survey_id) REFERENCES survey(survey_id)
) WITHOUT ROWID;

-- Mobile device ------------------------------------------------------

CREATE TABLE mobile_device_type_lut (
    mobile_device_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mobile_device_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mobile_device (
    mobile_device_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mobile_device_type_id text NOT NULL,
    mobile_equipment_identifier text NOT NULL,
    mobile_device_name text NOT NULL,
    mobile_device_description text NOT NULL,
    active_indicator integer NOT NULL,
    inactive_datetime text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_mobile_device_type_lut__mobile_device FOREIGN KEY (mobile_device_type_id) REFERENCES mobile_device_type_lut(mobile_device_type_id)
) WITHOUT ROWID;

-- Survey mobile device ------------------------------------------------------

CREATE TABLE survey_mobile_device (
    survey_mobile_device_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    mobile_device_id text NOT NULL,
    CONSTRAINT fk_survey__survey_mobile_device FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_mobile_device__survey_mobile_device FOREIGN KEY (mobile_device_id) REFERENCES mobile_device(mobile_device_id)
) WITHOUT ROWID;

-- Fish barrier ------------------------------------------------------

CREATE TABLE barrier_type_lut (
    barrier_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    barrier_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE barrier_measurement_type_lut (
    barrier_measurement_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    measurement_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_barrier (
    fish_barrier_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    barrier_location_id text NOT NULL,
    barrier_type_id text NOT NULL,
    barrier_observed_datetime text,
    barrier_height_meter decimal(4,2),
    barrier_height_type_id text NOT NULL,
    plunge_pool_depth_meter decimal(4,2),
    plunge_pool_depth_type_id text NOT NULL,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__fish_barrier FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_location__fish_barrier FOREIGN KEY (barrier_location_id) REFERENCES location(location_id),
    CONSTRAINT fk_barrier_measurement_type_lut__fish_barrier__height_type_id FOREIGN KEY (barrier_height_type_id) REFERENCES barrier_measurement_type_lut(barrier_measurement_type_id),
    CONSTRAINT fk_barrier_measurement_type_lut__fish_barrier__depth_type_id FOREIGN KEY (plunge_pool_depth_type_id) REFERENCES barrier_measurement_type_lut(barrier_measurement_type_id),
    CONSTRAINT fk_barrier_type_lut__fish_barrier FOREIGN KEY (barrier_type_id) REFERENCES barrier_type_lut(barrier_type_id)
) WITHOUT ROWID;

-- Waterbody measurement ------------------------------------------------------

CREATE TABLE water_clarity_type_lut (
    water_clarity_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    clarity_type_short_description text NOT NULL,
    clarity_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE waterbody_measurement (
    waterbody_measurement_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    water_clarity_type_id text,
    water_clarity_meter decimal(4,2),
    stream_flow_measurement_cfs integer,
    start_water_temperature_datetime text,
    start_water_temperature_celsius decimal(4,1),
    end_water_temperature_datetime text,
    end_water_temperature_celsius decimal(4,1),
    waterbody_ph decimal(2,1),
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__waterbody_measurement FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_water_clarity_type_lut__waterbody_measurement FOREIGN KEY (water_clarity_type_id) REFERENCES water_clarity_type_lut(water_clarity_type_id)
) WITHOUT ROWID;

-- Other observation ------------------------------------------------------

CREATE TABLE observation_type_lut (
    observation_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    observation_type_name text NOT NULL,
    observation_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE other_observation (
    other_observation_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    observation_location_id text,
    observation_type_id text NOT NULL,
    observation_datetime text,
    observation_count integer,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__other_observation FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_location__other_observation FOREIGN KEY (observation_location_id) REFERENCES location(location_id),
    CONSTRAINT fk_observation_type_lut__other_observation FOREIGN KEY (observation_type_id) REFERENCES observation_type_lut(observation_type_id)
) WITHOUT ROWID;

-- Fish capture ------------------------------------------------------

CREATE TABLE gear_performance_type_lut (
    gear_performance_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    performance_short_description text NOT NULL,
    performance_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_capture (
    fish_capture_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    gear_performance_type_id text NOT NULL,
    fish_start_datetime text,
    fish_end_datetime text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__fish_capture FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_gear_performance_type_lut__fish_capture FOREIGN KEY (gear_performance_type_id) REFERENCES gear_performance_type_lut(gear_performance_type_id)
) WITHOUT ROWID;

-- Fish presence ------------------------------------------------------

CREATE TABLE fish_presence_type_lut (
    fish_presence_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_presence_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_species_presence (
    fish_species_presence_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    species_id text NOT NULL,
    fish_presence_type_id text NOT NULL,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__fish_species_presence FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_species_lut__fish_species_presence FOREIGN KEY (species_id) REFERENCES species_lut(species_id),
    CONSTRAINT fk_fish_presence_type_lut__fish_species_presence FOREIGN KEY (fish_presence_type_id) REFERENCES fish_presence_type_lut(fish_presence_type_id)
) WITHOUT ROWID;

-- Create tables: Survey event Level -----------------------------------------------------------------------------------------------------

CREATE TABLE survey_design_type_lut (
    survey_design_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_design_type_code text NOT NULL,
    survey_design_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE cwt_detection_method_lut (
    cwt_detection_method_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    detection_method_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE survey_event (
    survey_event_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_id text NOT NULL,
    species_id text NOT NULL,
    survey_design_type_id text NOT NULL,
    cwt_detection_method_id text NOT NULL,
    run_id text NOT NULL,
    run_year integer NOT NULL,
    estimated_percent_fish_seen integer,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey__survey_event FOREIGN KEY (survey_id) REFERENCES survey(survey_id),
    CONSTRAINT fk_species_lut__survey_event FOREIGN KEY (species_id) REFERENCES species_lut(species_id),
    CONSTRAINT fk_survey_design_type_lut__survey_event FOREIGN KEY (survey_design_type_id) REFERENCES survey_design_type_lut(survey_design_type_id),
    CONSTRAINT fk_cwt_detection_method_lut__survey_event FOREIGN KEY (cwt_detection_method_id) REFERENCES cwt_detection_method_lut(cwt_detection_method_id),
    CONSTRAINT fk_run_lut__survey_event FOREIGN KEY (run_id) REFERENCES run_lut(run_id)
) WITHOUT ROWID;

-- Create tables: Fish encounter Level -----------------------------------------------------------------------------------------------------

-- Fish encounter ------------------------------------------------------

CREATE TABLE fish_status_lut (
    fish_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE sex_lut (
    sex_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    sex_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE maturity_lut (
    maturity_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    maturity_short_description text NOT NULL,
    maturity_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE origin_lut (
    origin_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    origin_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE cwt_detection_status_lut (
    cwt_detection_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    detection_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

--CREATE TABLE adipose_clip_status_lut (
--	adipose_clip_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
--    adipose_clip_status_code text NOT NULL,
--    adipose_clip_status_description text NOT NULL,
--    obsolete_flag text NOT NULL,
--    obsolete_datetime text
--) WITHOUT ROWID;

CREATE TABLE fish_behavior_type_lut (
    fish_behavior_type_id text DEFAULT (Createtext()) PRIMARY KEY,
    behavior_short_description text NOT NULL,
    behavior_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mortality_type_lut (
    mortality_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mortality_type_short_description text NOT NULL,
    mortality_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_encounter (
    fish_encounter_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_event_id text NOT NULL,
    fish_location_id text,
    fish_status_id text NOT NULL,
    sex_id text NOT NULL,
    maturity_id text NOT NULL,
    origin_id text NOT NULL,
    cwt_detection_status_id text NOT NULL,
    adipose_clip_status_id text NOT NULL,
    fish_behavior_type_id text NOT NULL,
    mortality_type_id text NOT NULL,
    fish_encounter_datetime text,
    fish_count integer NOT NULL,
    previously_counted_indicator integer NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey_event__fish_encounter FOREIGN KEY (survey_event_id) REFERENCES survey_event(survey_event_id),
    CONSTRAINT fk_location__fish_encounter FOREIGN KEY (fish_location_id) REFERENCES location(location_id),
    CONSTRAINT fk_fish_status_lut__fish_encounter FOREIGN KEY (fish_status_id) REFERENCES fish_status_lut(fish_status_id),
    CONSTRAINT fk_sex_lut__fish_encounter FOREIGN KEY (sex_id) REFERENCES sex_lut(sex_id),
    CONSTRAINT fk_maturity_lut__fish_encounter FOREIGN KEY (maturity_id) REFERENCES maturity_lut(maturity_id),
    CONSTRAINT fk_origin_lut__fish_encounter FOREIGN KEY (origin_id) REFERENCES origin_lut(origin_id),
    CONSTRAINT fk_cwt_detection_status_lut__fish_encounter FOREIGN KEY (cwt_detection_status_id) REFERENCES cwt_detection_status_lut(cwt_detection_status_id),
    CONSTRAINT fk_adipose_clip_status_lut__fish_encounter FOREIGN KEY (adipose_clip_status_id) REFERENCES adipose_clip_status_lut(adipose_clip_status_id),
    CONSTRAINT fk_fish_behavior_type_lut__fish_encounter FOREIGN KEY (fish_behavior_type_id) REFERENCES fish_behavior_type_lut(fish_behavior_type_id),
    CONSTRAINT fk_mortality_type_lut__fish_encounter FOREIGN KEY (mortality_type_id) REFERENCES mortality_type_lut(mortality_type_id)
) WITHOUT ROWID;

-- Individual fish ------------------------------------------------------

CREATE TABLE fish_condition_type_lut (
    fish_condition_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_condition_short_description text NOT NULL,
    fish_condition_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_trauma_type_lut (
    fish_trauma_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    trauma_type_short_description text NOT NULL,
    trauma_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE gill_condition_type_lut (
    gill_condition_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    gill_condition_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE spawn_condition_type_lut (
    spawn_condition_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    spawn_condition_short_description text NOT NULL,
    spawn_condition_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE cwt_result_type_lut (
    cwt_result_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    cwt_result_type_code text NOT NULL,
    cwt_result_type_short_description text NOT NULL,
    cwt_result_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE age_code_lut (
    age_code_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    european_age_code text NOT NULL,
    gilbert_rich_age_code text,
    fresh_water_annuli integer,
    maiden_salt_water_annuli integer,
    total_salt_water_annuli integer,
    age_at_spawning integer,
    prior_spawn_event_count integer,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text DEFAULT(datetime('now'))
) WITHOUT ROWID;

CREATE TABLE individual_fish (
    individual_fish_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_encounter_id text NOT NULL,
    fish_condition_type_id text NOT NULL,
    fish_trauma_type_id text NOT NULL,
    gill_condition_type_id text NOT NULL,
    spawn_condition_type_id text NOT NULL,
    cwt_result_type_id text NOT NULL,
    age_code_id text,
    percent_eggs_retained integer,
    eggs_retained_gram decimal(4,1),
    eggs_retained_number integer,
    fish_sample_number text,
    scale_sample_card_number text,
    scale_sample_position_number text,
    cwt_snout_sample_number text,
    cwt_tag_code text,
    genetic_sample_number text,
    otolith_sample_number text,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_fish_encounter__individual_fish FOREIGN KEY (fish_encounter_id) REFERENCES fish_encounter(fish_encounter_id),
    CONSTRAINT fk_fish_condition_type_lut__individual_fish FOREIGN KEY (fish_condition_type_id) REFERENCES fish_condition_type_lut(fish_condition_type_id),
    CONSTRAINT fk_fish_trauma_type_lut__individual_fish FOREIGN KEY (fish_trauma_type_id) REFERENCES fish_trauma_type_lut(fish_trauma_type_id),
    CONSTRAINT fk_gill_condition_type_lut__individual_fish FOREIGN KEY (gill_condition_type_id) REFERENCES gill_condition_type_lut(gill_condition_type_id),
    CONSTRAINT fk_spawn_condition_type_lut__individual_fish FOREIGN KEY (spawn_condition_type_id) REFERENCES spawn_condition_type_lut(spawn_condition_type_id),
    CONSTRAINT fk_cwt_result_type_lut__individual_fish FOREIGN KEY (cwt_result_type_id) REFERENCES cwt_result_type_lut(cwt_result_type_id),
    CONSTRAINT fk_age_code_lut__individual_fish FOREIGN KEY (age_code_id) REFERENCES age_code_lut(age_code_id)
) WITHOUT ROWID;

CREATE TABLE fish_length_measurement_type_lut (
    fish_length_measurement_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    length_type_code text NOT NULL,
    length_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_length_measurement (
    fish_length_measurement_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    individual_fish_id text NOT NULL,
    fish_length_measurement_type_id text NOT NULL,
    length_measurement_centimeter decimal(6,2) NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_individual_fish__fish_length_measurement FOREIGN KEY (individual_fish_id) REFERENCES individual_fish(individual_fish_id),
    CONSTRAINT fk_fish_length_measurement_type_lut__fish_length_measurement FOREIGN KEY (fish_length_measurement_type_id) REFERENCES fish_length_measurement_type_lut(fish_length_measurement_type_id)
) WITHOUT ROWID;

-- Fish capture event ------------------------------------------------------

CREATE TABLE fish_capture_status_lut (
    fish_capture_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_capture_status_code text NOT NULL,
    fish_capture_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE disposition_type_lut (
    disposition_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    disposition_type_code text NOT NULL,
    disposition_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE disposition_lut (
    disposition_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    disposition_code text NOT NULL,
    fish_books_code text,
    disposition_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_capture_event (
    fish_capture_event_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_encounter_id text NOT NULL,
    fish_capture_status_id text NOT NULL,
    disposition_type_id text NOT NULL,
    disposition_id text NOT NULL,
    disposition_location_id text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_fish_encounter__fish_capture_event FOREIGN KEY (fish_encounter_id) REFERENCES fish_encounter(fish_encounter_id),
    CONSTRAINT fk_fish_capture_status_lut__fish_capture_event FOREIGN KEY (fish_capture_status_id) REFERENCES fish_capture_status_lut(fish_capture_status_id),
    CONSTRAINT fk_disposition_type_lut__fish_capture_event FOREIGN KEY (disposition_type_id) REFERENCES disposition_type_lut(disposition_type_id),
    CONSTRAINT fk_disposition_lut__fish_capture_event FOREIGN KEY (disposition_id) REFERENCES disposition_lut(disposition_id),
    CONSTRAINT fk_location__fish_capture_event FOREIGN KEY (disposition_location_id) REFERENCES location(location_id)
) WITHOUT ROWID;

-- Fish mark ------------------------------------------------------

CREATE TABLE mark_type_category_lut (
    mark_type_category_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_type_category_name text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_type_lut (
    mark_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_type_category_id text NOT NULL,
    mark_type_code text NOT NULL,
    mark_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text,
    CONSTRAINT fk_mark_type_category_lut__mark_type_lut FOREIGN KEY (mark_type_category_id) REFERENCES mark_type_category_lut(mark_type_category_id)
) WITHOUT ROWID;

CREATE TABLE mark_status_lut (
    mark_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_orientation_lut (
    mark_orientation_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_orientation_code text NOT NULL,
    mark_orientation_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_placement_lut (
    mark_placement_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_placement_code text NOT NULL,
    mark_placement_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_size_lut (
    mark_size_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_size_code text NOT NULL,
    mark_size_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_color_lut (
    mark_color_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_color_code text NOT NULL,
    mark_color_name text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE mark_shape_lut (
    mark_shape_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    mark_shape_code text NOT NULL,
    mark_shape_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE fish_mark (
    fish_mark_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    fish_encounter_id text NOT NULL,
    mark_type_id text NOT NULL,
    mark_status_id text NOT NULL,
    mark_orientation_id text NOT NULL,
    mark_placement_id text NOT NULL,
    mark_size_id text NOT NULL,
    mark_color_id text NOT NULL,
    mark_shape_id text NOT NULL,
    tag_number text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_fish_encounter__fish_mark FOREIGN KEY (fish_encounter_id) REFERENCES fish_encounter(fish_encounter_id),
    CONSTRAINT fk_mark_type_lut__fish_mark FOREIGN KEY (mark_type_id) REFERENCES mark_type_lut(mark_type_id),
    CONSTRAINT fk_mark_status_lut__fish_mark FOREIGN KEY (mark_status_id) REFERENCES mark_status_lut(mark_status_id),
    CONSTRAINT fk_mark_orientation_lut__fish_mark FOREIGN KEY (mark_orientation_id) REFERENCES mark_orientation_lut(mark_orientation_id),
    CONSTRAINT fk_mark_placement_lut__fish_mark FOREIGN KEY (mark_placement_id) REFERENCES mark_placement_lut(mark_placement_id),
    CONSTRAINT fk_mark_size_lut__fish_mark FOREIGN KEY (mark_size_id) REFERENCES mark_size_lut(mark_size_id),
    CONSTRAINT fk_mark_color_lut__fish_mark FOREIGN KEY (mark_color_id) REFERENCES mark_color_lut(mark_color_id),
    CONSTRAINT fk_mark_shape_lut__fish_mark FOREIGN KEY (mark_shape_id) REFERENCES mark_shape_lut(mark_shape_id)
) WITHOUT ROWID;

-- Create tables: Redd encounter Level -----------------------------------------------------------------------------------------------------

-- Redd encounter ------------------------------------------------------

CREATE TABLE redd_status_lut (
    redd_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    redd_status_code text NOT NULL,
    redd_status_short_description text NOT NULL,
    redd_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE redd_encounter (
    redd_encounter_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    survey_event_id text NOT NULL,
    redd_location_id text,
    redd_status_id text NOT NULL,
    redd_encounter_datetime text,
    redd_count integer NOT NULL,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_survey_event__redd_encounter FOREIGN KEY (survey_event_id) REFERENCES survey_event(survey_event_id),
    CONSTRAINT fk_location__redd_encounter FOREIGN KEY (redd_location_id) REFERENCES location(location_id),
    CONSTRAINT fk_redd_status_lut__redd_encounter FOREIGN KEY (redd_status_id) REFERENCES redd_status_lut(redd_status_id)
) WITHOUT ROWID;

-- Individual redd ------------------------------------------------------

CREATE TABLE redd_shape_lut (
    redd_shape_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    redd_shape_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE redd_dewatered_type_lut (
    redd_dewatered_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    dewatered_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE individual_redd (
    individual_redd_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    redd_encounter_id text NOT NULL,
    redd_shape_id text NOT NULL,
    redd_dewatered_type_id text,
    percent_redd_visible integer,
    redd_length_measure_meter decimal(4,2),
    redd_width_measure_meter decimal(4,2),
    redd_depth_measure_meter decimal(3,2),
    tailspill_height_measure_meter decimal(3,2),
    percent_redd_superimposed integer,
    percent_redd_degraded integer,
    superimposed_redd_name text,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_redd_encounter__individual_redd FOREIGN KEY (redd_encounter_id) REFERENCES redd_encounter(redd_encounter_id),
    CONSTRAINT fk_redd_shape_lut__individual_redd FOREIGN KEY (redd_shape_id) REFERENCES redd_shape_lut(redd_shape_id),
    CONSTRAINT fk_redd_dewatered_type_lut__individual_redd FOREIGN KEY (redd_dewatered_type_id) REFERENCES redd_dewatered_type_lut(redd_dewatered_type_id)
) WITHOUT ROWID;

-- Redd substrate ------------------------------------------------------

CREATE TABLE substrate_level_lut (
    substrate_level_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    substrate_level_short_description text NOT NULL,
    substrate_level_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE substrate_type_lut (
    substrate_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    substrate_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE redd_substrate (
    redd_substrate_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    redd_encounter_id text NOT NULL,
    substrate_level_id text NOT NULL,
    substrate_type_id text NOT NULL,
    substrate_percent integer NOT NULL,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_redd_encounter__redd_substrate FOREIGN KEY (redd_encounter_id) REFERENCES redd_encounter(redd_encounter_id),
    CONSTRAINT fk_substrate_level_lut__redd_substrate FOREIGN KEY (substrate_level_id) REFERENCES substrate_level_lut(substrate_level_id),
    CONSTRAINT fk_substrate_type_lut__redd_substrate FOREIGN KEY (substrate_type_id) REFERENCES substrate_type_lut(substrate_type_id)
) WITHOUT ROWID;

-- Redd confidence ------------------------------------------------------

CREATE TABLE redd_confidence_type_lut (
    redd_confidence_type_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    confidence_type_short_description text NOT NULL,
    confidence_type_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE redd_confidence_review_status_lut (
    redd_confidence_review_status_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    review_status_description text NOT NULL,
    obsolete_flag integer NOT NULL,
    obsolete_datetime text
) WITHOUT ROWID;

CREATE TABLE redd_confidence (
    redd_confidence_id text DEFAULT (CreateUUID()) PRIMARY KEY,
    redd_encounter_id text NOT NULL,
    redd_confidence_type_id text NOT NULL,
    redd_confidence_review_status_id text NOT NULL,
    comment_text text,
    created_datetime text DEFAULT (datetime('now')) NOT NULL,
    created_by text NOT NULL,
    modified_datetime text,
    modified_by text,
    CONSTRAINT fk_redd_encounter__redd_confidence FOREIGN KEY (redd_encounter_id) REFERENCES redd_encounter(redd_encounter_id),
    CONSTRAINT fk_redd_confidence_type_lut__redd_confidence FOREIGN KEY (redd_confidence_type_id) REFERENCES redd_confidence_type_lut(redd_confidence_type_id),
    CONSTRAINT fk_redd_confidence_review_status_lut__redd_confidence FOREIGN KEY (redd_confidence_review_status_id) REFERENCES redd_confidence_review_status_lut(redd_confidence_review_status_id)
) WITHOUT ROWID;

