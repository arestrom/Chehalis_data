
-- originally was meuse.sqlite
CREATE TABLE 'meuse.db' (
  ogc_fid INTEGER PRIMARY KEY,
  'GEOMETRY' BLOB,
  'cadmium' FLOAT,
  'copper' FLOAT,
  'lead' FLOAT,
  'zinc' FLOAT,
  'elev' FLOAT,
  'dist' FLOAT,
  'om' FLOAT,
  'ffreq' VARCHAR,
  'soil' VARCHAR,
  'lime' VARCHAR,
  'landuse' VARCHAR,
  'dist.m' FLOAT
)
