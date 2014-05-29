#!/bin/env python

import subprocess
import simplejson as json

# TODO: parameterize
# existing table for which the AVRO should be generated
TBL_NAME="<db>.<tbl>"

process = subprocess.Popen([ 'hive -e "DESC %s" 2>/dev/null' % TBL_NAME ],
                           shell=True,
                           stdin=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE,
                          )
output =  process.communicate()[0].strip()

def gen_avro_field(hive_schema_line):
  print hive_schema_line
  name, type = hive_schema_line.strip().split('\t')
  return '{ "name": "%s", "type": "%s" } ' % ( name, type)

lst = [ gen_avro_field(line) for line in output.split('\n') ]
schema_literal = """{
                "namespace": "jmpc",
                "name": "curves",
                "type": "record",
                "fields": [
                                %s
                ]
}""" % ( ',\n\t\t'.join( lst ) )

print schema_literal
