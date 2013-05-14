package com.continuuity.performance.benchmark;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.performance.gateway.LoadGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that manages the ingestion of data into a stream.
 */
public class StreamRunner {
  private static final Logger LOG = LoggerFactory.getLogger(StreamRunner.class);
  private static final String LOAD_GENERATOR_BENCH = LoadGenerator.class.getName();

  private final String streamName;
  private CConfiguration config = CConfiguration.create();

  public StreamRunner(String streamName) {
    this.streamName = streamName;
  }

  public void run(long iteration) throws Exception {
    String[] args1 = new String[8];
    args1[0] = "--bench";
    args1[1] = LOAD_GENERATOR_BENCH;
    args1[2] = "--runs";
    args1[3] = String.valueOf(iteration);
    args1[4] = "--gateway"; // args1[4] = "--base";
    args1[5] = "localhost"; // args1[5] = "http://hostname:10000/wordStream/";
    args1[6] = "--stream";
    args1[7] = streamName;

    BenchmarkRunner.main(args1);
  }
}
