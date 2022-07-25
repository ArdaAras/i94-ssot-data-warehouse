CREATE TABLE public.dim_time (
    year decimal(6, 1) NOT NULL,
    month int4,
    day int4,
    week int4,
    weekday int4,
    primary key(year)
);
