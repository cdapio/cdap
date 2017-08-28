# coding=utf-8
# Copyright © 2017 Cask Data, Inc.

# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

from cdap.pyspark import SparkExecutionContext

sc = SparkContext()
sql = SQLContext(sc)

sec = SparkExecutionContext()
metrics = sec.getMetrics()

streamName = sec.getRuntimeArguments()['input.stream']

def isEven(body):
  metrics.count("body", 1)
  return int(body.split(' ')[1]) % 2 == 0

sql.udf.register("isEven", isEven, BooleanType())

sql.sql("SELECT body FROM cdapstream." + streamName + " WHERE isEven(body)=True") \
  .coalesce(1) \
  .write.format("text").save(sec.getRuntimeArguments().get("output.path"))
