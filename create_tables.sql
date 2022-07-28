CREATE TABLE IF NOT EXISTS public.dim_time (
    timestamp float NOT NULL,
    year int,
    month int,
    day int,
    week int,
    weekday int
);

CREATE TABLE IF NOT EXISTS public.dim_temperatures (
    city varchar(256),
    avg_temp float,
    avg_temp_uncertainty float,
    PRIMARY KEY (city)
);
