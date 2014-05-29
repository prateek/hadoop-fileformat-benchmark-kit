#!/bin/bash

## TODO: this relies on bash4 for the associative arrays
## TODO: Extract out to have a better configuration framework
## TODO: do testing based on CDH deployment version,
##      i.e. each framework supports different fileformats and compressions
##           at different times. Make the configuration be version specific

declare -A TABLE_TYPES
TABLE_TYPES=( \
["csv"]="STORED AS TEXTFILE"     \
["rc"]="STORED AS RCFILE"        \
["seq"]="STORED AS SEQUENCEFILE" \
["avro"]=$(cat <<EOF
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
  'avro.schema.url'='hdfs:///user/f557905/curves_schema.avsc');
EOF
## TODO: ^ make avro schema generation be dynamic
) \
## TODO: make parquet query generation be dynamic
## TODO: (!) `Create Table AS` should be fixed with PARQUET, file the jira and fix
["parquet"]=$(cat ./parquet-table-snippet.hql)
)

declare -A INSERT_APPEND
INSERT_APPEND=( \
["csv"]="AS SELECT * FROM raw_data;" \
["rc"]="AS SELECT * FROM raw_data;"  \
["seq"]="AS SELECT * FROM raw_data;" \
["avro"]=$(cat <<"EOF"

INSERT OVERWRITE TABLE TABLE_NAME
SELECT * FROM
raw_data;
EOF
) \
["parquet"]=$(cat <<"EOF"

INSERT OVERWRITE TABLE TABLE_NAME
SELECT * FROM
raw_data;
EOF
) \
)

## Sources: https://www.inkling.com/read/hadoop-definitive-guide-tom-white-3rd/chapter-4/compression
## BZIP is a pita, not supporting it atm
# ["csv"]="uncompressed deflate gzip snappy"  \
## RC has a bug with bzip2 - https://issues.apache.org/jira/browse/HIVE-4788
# ["rc"]="uncompressed deflate gzip snappy"   \
# ["seq"]="uncompressed deflate gzip snappy"  \
## Avro doesn't support all codecs until 1.7.4 - https://issues.apache.org/jira/browse/AVRO-1243
# ["avro"]="uncompressed deflate snappy" \
## Parquet only suports Gzip, Snappy & uncompressed
# ["parquet"]="uncompressed gzip snappy"  \
declare -A COMPATIBLE_CODECS
COMPATIBLE_CODECS=( \
["csv"]="uncompressed deflate gzip snappy"  \
["rc"]="uncompressed deflate gzip snappy"   \
["seq"]="uncompressed deflate gzip snappy"  \
["avro"]="uncompressed deflate snappy"      \
["parquet"]="uncompressed gzip snappy"      \
)

# check available codecs `hive -e 'set io.compression.codecs'`
declare -A COMPRESSION
COMPRESSION=( ["uncompressed"]=$(cat <<EOF
SET hive.exec.compress.output=false;
SET mapreduce.output.fileoutputformat.compress=false;
set mapred.output.compression.type=NONE;
SET parquet.compression=UNCOMPRESSED;
EOF
) \
["deflate"]=$(cat <<EOF
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET avro.output.codec=deflate;
set mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.DeflateCodec;
EOF
) \
["gzip"]=$(cat <<EOF
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET avro.output.codec=gzip;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET parquet.compression=GZIP;
EOF
) \
["snappy"]=$(cat <<EOF
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET avro.output.codec=snappy;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET parquet.compression=SNAPPY;
EOF
) \
["bzip2"]=$(cat <<EOF
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET avro.output.codec=bzip2;
SET mapred.output.compression.type=BLOCK;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
EOF
) \
)

for tt in "${!TABLE_TYPES[@]}"
do

IFS=' ' read -a supported_codecs <<< "${COMPATIBLE_CODECS[$tt]}"

for co in "${supported_codecs[@]}"
do

table_name=${tt}_${co}
(cat - | sed "s/TABLE_NAME/${table_name}/g" | tee ./curve-gen/$table_name.hql ) <<EOF

-- FORMAT: $tt, COMPRESSION: $co
USE masvit_curves;

-- SET COMPRESSION
${COMPRESSION["$co"]}

DROP TABLE IF EXISTS TABLE_NAME;

CREATE TABLE TABLE_NAME
${TABLE_TYPES["$tt"]}
${INSERT_APPEND["$tt"]}
EOF

done
done
