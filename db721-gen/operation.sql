DROP foreign data wrapper IF EXISTS polo_wrapper CASCADE;

CREATE foreign data wrapper polo_wrapper
  handler polo_fdw_handler
  validator polo_fdw_validator;

CREATE SERVER IF NOT EXISTS polo_server
  foreign data wrapper polo_wrapper;

CREATE FOREIGN TABLE IF NOT EXISTS db721_farm
(                                   
    farm_name       varchar,
    min_age_weeks   real,
    max_age_weeks   real
) SERVER polo_server OPTIONS
(
    tablename 'Farm'
);
