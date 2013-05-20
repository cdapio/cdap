package com.continuuity.performance.application;

/**
 *
 */
public interface MetricsReporter  {

  /* metric upload format:
  put operation "put <metric> <epoch timestamp> <double value> <tag1=value1> <tag2=value2> ... <tag8=value8>
  i.e. put benchmark.ops.per_sec.10s 1368002176 16740.00 benchmark=QueueBenchmark
                                                         operation=producer
                                                         threadCount=1
                                                         tcbuildnum=610
                                                         edition=continuuity-cloud-edition
                                                         version=1.4.1-SNAPSHOT
                                                         buildnum=124
                                                         build=1.4.1-20130425.071235-124
   */
}
