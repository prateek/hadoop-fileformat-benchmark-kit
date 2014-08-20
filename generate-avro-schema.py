#!/bin/env python

import subprocess
import simplejson as json
import sys

# TODO: parameterize
# existing table for which the AVRO should be generated
if len( sys.argv ) != 2:
    print "Usage: %s [input-db.]<input-table-name>" % sys.argv[0]
    sys.exit(1)

TBL_NAME=sys.argv[1]

process = subprocess.Popen([ 'hive -e "DESC %s" 2>/dev/null' % TBL_NAME ],
                           shell=True,
                           stdin=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                          )
output =  process.communicate()[0].strip()

def gen_avro_field(hive_schema_line):
  # print hive_schema_line
  name, type, _ = hive_schema_line.strip().split('\t')
  return '{ "name": "%s", "type": "%s" } ' % ( name, type)

lst = [ gen_avro_field(line) for line in output.split('\n') if len( line.strip() ) != 0 and line[0] != '#' ]

schema_literal = """{
                "namespace": "fileformat.benchmark",
                "name": "BenchmarkRecord",
                "type": "record",
                "fields": [
                                %s
                ]
}""" % ( ',\n\t\t'.join( lst ) )

print schema_literal
