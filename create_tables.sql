CREATE TABLE public.dim_time (
    timestamp decimal(6, 1) NOT NULL,
    year int4,
    month int4,
    day int4,
    week int4,
    weekday int4,
    primary key(timestamp)
);
