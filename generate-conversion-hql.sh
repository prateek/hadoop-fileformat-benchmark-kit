#!/bin/bash
# currently used against CDH5.1

if [ $# -ne 2 ];
then
  echo "Usage: $0 [input-db.]<input-table-name> <output-table-prefix>"
  exit 1
fi

INPUT_TABLE=$1
OUTPUT_TABLE_PREFIX=$2

BASE_SCHEMA_NAME=$(date +%s)-BenchmarkSchema.avsc
SCHEMA_FILE=$(mktemp -d)/$BASE_SCHEMA_NAME

echo "Schema-file: $SCHEMA_FILE"
./generate-avro-schema.py $INPUT_TABLE > $SCHEMA_FILE

hdfs dfs -put $SCHEMA_FILE /tmp/$BASE_SCHEMA_NAME

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
  'avro.schema.url'='hdfs:///tmp/$BASE_SCHEMA_NAME');
EOF
) \
## TODO: make parquet query generation be dynamic
["parquet"]="STORED AS PARQUET" \
)

declare -A INSERT_APPEND
INSERT_APPEND=( \
["csv"]="AS SELECT * FROM $INPUT_TABLE;" \
["rc"]="AS SELECT * FROM $INPUT_TABLE;"  \
["seq"]="AS SELECT * FROM $INPUT_TABLE;" \
["avro"]=$(cat <<EOF

INSERT OVERWRITE TABLE TABLE_NAME
SELECT * FROM
$INPUT_TABLE;
EOF
) \
["parquet"]=$(cat <<EOF

INSERT OVERWRITE TABLE TABLE_NAME
SELECT * FROM
$INPUT_TABLE;
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

mkdir -p ./generated-queries

for tt in "${!TABLE_TYPES[@]}"
do

IFS=' ' read -a supported_codecs <<< "${COMPATIBLE_CODECS[$tt]}"

for co in "${supported_codecs[@]}"
do

table_name=$OUTPUT_TABLE_PREFIX_${tt}_${co}
(cat - | sed "s/TABLE_NAME/${table_name}/g" | tee ./generated-queries/$table_name.hql ) <<EOF

-- FORMAT: $tt, COMPRESSION: $co

CREATE DATABASE IF NOT EXISTS fileformat_benchmark;
USE fileformat_benchmark;

-- SET COMPRESSION
${COMPRESSION["$co"]}

DROP TABLE IF EXISTS TABLE_NAME;

CREATE TABLE TABLE_NAME
${TABLE_TYPES["$tt"]}
${INSERT_APPEND["$tt"]}
EOF

done
done
