rdf2csv--Python--Postgres-
==========================

I wanted to use Postgres to process some RDF data, but found that using individual INSERT INTO statements was much too slow. 
Postgres' COPY INTO mechanism is much, much faster. But it only works on valid CSV files.

RDF's standard syntax makes it not hard to transform to CSV, but it has quirks and a rigid importer like Postgres will not
tolerate anything but perfectly compliant CSV.

Googling around for a convert produced a C-library that is supposed to do a good conversion (http://librdf.org/rasqal/), but
that scared off C-phobia me. I also found the BigQuery Linked Data project (http://code.google.com/p/bigquery-linkeddata/)
which includes a Python-based RDF to CSV converter.

This is a fork of the BigQuery project, tweaked to produce Postgres compliant CSV (instead of BigQuery compliant) and to 
use rdflib for parsing to deal better with RDF quirks.

I did this in under an hour, so pull requests are welcome (for example, to let the user specify the database format to target).

@rogueleaderr