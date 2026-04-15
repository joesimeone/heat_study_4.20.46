library(duckdb)
library(tidyverse)


con <- dbConnect(duckdb())

# Enable HTTP filesystem and S3 support
dbExecute(con, "INSTALL httpfs")
dbExecute(con, "LOAD httpfs")

# Public bucket — anonymous access
dbExecute(con, "SET s3_region = 'us-west-2'")
dbExecute(con, "SET s3_access_key_id = ''")
dbExecute(con, "SET s3_secret_access_key = ''")

base_path <- "s3://oedi-data-lake/nrel-pds-building-stock/
  end-use-load-profiles-for-us-building-stock/
  2025/resstock_amy2018_release_1/
  timeseries_individual_buildings/
  by_state/upgrade=32/"

pa_path <- "s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2025/resstock_amy2018_release_1/timeseries_individual_buildings/by_state/upgrade=32/state=PA/*.parquet"
cat(pa_path)


schema <- dbGetQuery(con, sprintf(
  "DESCRIBE SELECT * FROM read_parquet('%s') LIMIT 0",
  pa_path
))
print(schema)


list.files

?sprintf


sprintf(
  "DESCRIBE SELECT * FROM read_parquet('%s') LIMIT 0",
  pa_path
)

write_csv(schema, 'nrel_bucekt_schema.csv')

hmm <- read_csv('nrel_bucekt_schema.csv')

query <- "
SELECT
  bldg_id,
  timestamp,

  -- heating energy (all fuels)
  \"out.load.heating.energy_delivered..kbtu\",


  -- cooling energy
  \"out.load.cooling.energy_delivered..kbtu\",
  \"out.unmet_hours.cooling..hour\",

  -- indoor temperature & humidity
  \"out.indoor_temperature.conditioned_space..c\",
  \"out.indoor_operative_temperature.conditioned_space..c\",
  \"out.indoor_radiant_temperature.conditioned_space..c\",
  \"out.indoor_dewpoint_temperature.conditioned_space..c\",
  \"out.indoor_relative_humidity.conditioned_space..percentage\",
  \"out.indoor_humidity_ratio.conditioned_space..kgwater_per_kgdryair\",

  -- outdoor temperature & humidity
  \"out.outdoor_air_drybulb_temp..c\",
  \"out.outdoor_air_wetbulb_temp..c\",
  \"out.outdoor_air_relative_humidity..percentage\",
  \"out.outdoor_humidity_ratio..kgwater_per_kgdryair\"

FROM read_parquet('s3://oedi-data-lake/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2025/resstock_amy2018_release_1/timeseries_individual_buildings/by_state/upgrade=29/state=PA/*.parquet')
LIMIT 50
"

df <- dbGetQuery(con, query)


write_csv(df, 'pa_sample_from_nrel_bucket.csv')
