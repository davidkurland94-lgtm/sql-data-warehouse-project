-- Table: logging.load_stats

-- DROP TABLE IF EXISTS logging.load_stats;

CREATE TABLE IF NOT EXISTS logging.load_stats
(
    log_id integer NOT NULL DEFAULT nextval('logging.load_stats_log_id_seq'::regclass),
    process_name text COLLATE pg_catalog."default" NOT NULL,
    table_name text COLLATE pg_catalog."default" NOT NULL,
    rows_inserted integer,
    start_ts timestamp without time zone NOT NULL,
    end_ts timestamp without time zone NOT NULL,
    duration_seconds numeric(10,2),
    status text COLLATE pg_catalog."default" DEFAULT 'SUCCESS'::text,
    executed_by text COLLATE pg_catalog."default" DEFAULT CURRENT_USER,
    CONSTRAINT load_stats_pkey PRIMARY KEY (log_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS logging.load_stats
    OWNER to postgres;
