package com.continuuity;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.data.cube.Cube;
import com.continuuity.data.cube.CubeConfiguration;
import com.continuuity.flows.EventProcessor;
import com.continuuity.procedures.Analytics;
import com.continuuity.procedures.Search;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HayStackApp implements Application {

  public static final long AGGREGATION_INTERVAL = 1L;
  public static final TimeUnit AGGREGATION_TIMEUNIT = TimeUnit.HOURS;
  public static final String AGGREGATION_DIMENSION_HOST = "hostname";
  public static final String AGGREGATION_DIMENSION_CLASS = "class";
  public static final String AGGREGATION_DIMENSION_COMPONENT = "component";
  public static final String AGGREGATION_DIMENSION_LEVEL = "level";

  public static final byte[] LOG_KEY = Bytes.toBytes("log");
  public static final byte[] ALERT_KEY_WARN = Bytes.toBytes("alertWarn");
  public static final byte[] ALERT_KEY_ERR = Bytes.toBytes("alertError");

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
                                   .setName("Haystack")
                                   .setDescription("Application to process and analyze logs")
                                   .withStreams()
                                    .add(new Stream("event-stream"))
                                   .withDataSets()
                                     .add(new SimpleTimeseriesTable("dataStore"))
                                     .add(new Cube("logAnalytics", LOG_ANALYTICS_CUBE))
                                   .withFlows()
                                     .add(new EventProcessor())
                                   .withProcedures()
                                     .add(new Search())
                                     .add(new Analytics())
                                   .noMapReduce()
                                   .noWorkflow()
                                   .build();
  }

  public static final CubeConfiguration LOG_ANALYTICS_CUBE =
    CubeConfiguration.Builder.with()
      .factsOrderedByTime(false)
      .dimensions()
        .string(AGGREGATION_DIMENSION_HOST)
        .string(AGGREGATION_DIMENSION_COMPONENT)
        .string(AGGREGATION_DIMENSION_CLASS)
        .string(AGGREGATION_DIMENSION_LEVEL)
      .aggregates()
        .count(AGGREGATION_DIMENSION_HOST)
        .hourly()
        .count(AGGREGATION_DIMENSION_HOST, AGGREGATION_DIMENSION_COMPONENT)
        .hourly()
        .count(AGGREGATION_DIMENSION_HOST, AGGREGATION_DIMENSION_LEVEL)
        .hourly()
        .count(AGGREGATION_DIMENSION_HOST, AGGREGATION_DIMENSION_COMPONENT, AGGREGATION_DIMENSION_CLASS)
        .hourly()
        .count(AGGREGATION_DIMENSION_LEVEL)
        .hourly()
        .count(AGGREGATION_DIMENSION_CLASS)
        .hourly()
      .build();
}
