-- Test table for Ska.DBI

CREATE TABLE ska_dbi_test_table (
  id    int not null,
  tstart float(16),
  tstop  float(16),
  datestart varchar(21),
  datestop  varchar(21),
  obsid  int not null,
  obi    int,
  pcad_mode char(4),
  aspect_mode varchar(12),
  sim_mode char(6),
  CONSTRAINT pk_id PRIMARY KEY (id)
) 
;

CREATE INDEX obi_idx ON ska_dbi_test_table (obi)
