CREATE TABLE IF NOT EXISTS public.dim_time (
    sas_timestamp float,
    year int,
    month int,
    day int,
    week int,
    weekday int,
    PRIMARY KEY (sas_timestamp)
)

CREATE TABLE IF NOT EXISTS public.dim_temperatures (
    city varchar(256) distkey,
    avg_temp float,
    avg_temp_uncertainty float,
    PRIMARY KEY (city)
)

CREATE TABLE IF NOT EXISTS public.dim_cities (
    city varchar(256) distkey,
    state varchar(256),
    median_age float,
    male_pop int,
    female_pop int,
    total_pop int,
    veterans int,
    foreign_born int,
    avg_household_size float,
    PRIMARY KEY (city)
)

CREATE TABLE IF NOT EXISTS public.dim_ports (
    port_id varchar(32) distkey,
    name varchar(256),
    city varchar(256),
    type varchar(256),
    iata_code varchar(32),
    iso_region varchar(32),
    PRIMARY KEY (port_id)
)

CREATE TABLE IF NOT EXISTS public.fact_immigrations (
    immigration_id int,
    origin int,
    landing_port varchar(32),
    arrival_date float,
    departure_date float,
    arrival_mode int,
    city varchar(256) distkey,
    age  int,
    visa int,
    visatype varchar(32),
    gender varchar(32),
    PRIMARY KEY (immigration_id)
)
