# hadoop-fileformat-benchmark-kit
---------------------------------
Aims to be a set of utilities to assist benchmarking performance for different fileformats for a given workload (Hive/Impala). Attributes it cares about -

1. Size of blocks file
2. Compression Ratio
3. Query Performance - *pending item*

**Warning** this is a work in progress. At the moment, it does conversions for single tables using scripts

# Usage
-------
```sh
$ ./generate-conversion-hql.sh <input-db>.<input-table> <output-table-prefix> \
  > hive-bechmark.hql
$ hive -f hive-bechmark.hql
```

# Known Issues
--------------
- Avro conversion is not working at the moment

## References ##
- Presentation on file-formats http://www.slideshare.net/Hadoop_Summit/kamat-singh-june27425pmroom210cv2
