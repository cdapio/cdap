package com.continuuity.procedures;

import com.continuuity.HayStackApp;
import com.continuuity.api.Resources;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.data.common.DataPoint;
import com.continuuity.data.cube.Cube;
import com.continuuity.data.cube.CubeProcedure;
import com.continuuity.data.cube.CubeQuery;
import com.continuuity.data.cube.CubeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Cube analytics queries.
 */
public class Analytics extends CubeProcedure {

  @UseDataSet("logAnalytics")
  private Cube aggregator;

  private long TIME_24_HRS_MILLIS = 24 * 60 * 60 * 1000L;
  private static final Logger LOG  = LoggerFactory.getLogger(Analytics.class);
  @Override
  public Cube getCube() {
    return aggregator;
  }

  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName("Analytics")
      .setDescription("Queries the cube")
      .withResources(new Resources(256))
      .build();
  }


  @Handle("hostStats")
  public void hostStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String hostname = request.getArgument("hostname");

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (hostname == null || hostname.isEmpty()) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Hostname cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS,
                         getStatsSingleDimension(start, end, HayStackApp.AGGREGATION_DIMENSION_HOST, hostname));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }

  private List<DataPoint> getStatsSingleDimension(long start, long end,
                                                  String dimension, String value) {
    LOG.debug("Start {} end {}", new Date(start), new Date(end));
    CubeQuery query = getCube().buildQuery()
      .select()
      .count()
      .dimension(dimension)
      .value(value)
      .group()
      .hourly(start, end)
      .build();

    CubeResult result = aggregator.read(query);
    for (DataPoint data : result.getDataPoints()) {
      LOG.debug("Cube result analytics: {} {}", data.getTimestamp(), data.getCount());
    }
    return result.getDataPoints();
  }

  @Handle("hostComponentStats")
  public void hostComponentStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String hostname = request.getArgument("hostname");
    String component = request.getArgument("component");

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (hostname == null || hostname.isEmpty() ||
      component == null || component.isEmpty()) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Hostname and component cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, getStatsTwoDimension(start, end,
                                                                              HayStackApp.AGGREGATION_DIMENSION_HOST,
                                                                              hostname,
                                                                              HayStackApp.AGGREGATION_DIMENSION_COMPONENT,
                                                                              component));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }


  private List<DataPoint> getStatsTwoDimension(long start, long end,
                                               String dimension1, String value1,
                                               String dimension2, String value2){
    CubeQuery query = getCube().buildQuery()
      .select()
      .count()
      .dimension(dimension1)
      .value(value1)
      .dimension(dimension2)
      .value(value2)
      .group()
      .hourly(start, end)
      .build();

    CubeResult result = aggregator.read(query);
    return result.getDataPoints();
  }


  @Handle("hostLoglevelStats")
  public void hostLogLevelStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String hostname = request.getArgument("hostname");
    String level = request.getArgument("level");

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (hostname == null || hostname.isEmpty() ||
      level == null || level.isEmpty())  {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Hostname  level cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, getStatsTwoDimension(start, end,
                                                                              HayStackApp.AGGREGATION_DIMENSION_HOST,
                                                                              hostname,
                                                                              HayStackApp.AGGREGATION_DIMENSION_LEVEL,
                                                                              level));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }

  @Handle("hostComponentClassStats")
  public void hostComponentClassStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String hostname = request.getArgument("hostname");
    String component = request.getArgument("component");
    String className = request.getArgument("class");

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (hostname == null || hostname.isEmpty() ||
      component == null || component.isEmpty() ||
      className == null || className.isEmpty())  {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Hostname  component and className cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS,
                         getStatsThreeDimensions(start, end,
                                                 HayStackApp.AGGREGATION_DIMENSION_HOST, hostname,
                                                 HayStackApp.AGGREGATION_DIMENSION_COMPONENT, component,
                                                 HayStackApp.AGGREGATION_DIMENSION_CLASS, className));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }


  private List<DataPoint> getStatsThreeDimensions(long start, long end,
                                                  String dimension1, String value1,
                                                  String dimension2, String value2,
                                                  String dimension3, String value3){
    CubeQuery query = getCube().buildQuery()
      .select()
      .count()
      .dimension(dimension1)
       .value(value1)
      .dimension(dimension2)
       .value(value2)
      .dimension(dimension3)
       .value(value3)
      .group()
      .hourly(start, end)
      .build();

    CubeResult result = aggregator.read(query);
    return result.getDataPoints();
  }


  @Handle("classStats")
  public void classStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String className = request.getArgument("class");

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (className == null || className.isEmpty())  {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "class cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, getStatsSingleDimension(start, end,
                                                                                 HayStackApp.AGGREGATION_DIMENSION_CLASS,
                                                                                 className));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }

  @Handle("logLevelStats")
  public void logLevelStats(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");
    String level = request.getArgument("level");

    //HACK:
    level = level == null ? "WARN" : level;

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (level == null || level.isEmpty())  {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "level cannot be empty");
      return;
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    try {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, getStatsSingleDimension(start, end,
                                                                                 HayStackApp.AGGREGATION_DIMENSION_LEVEL,
                                                                                 level));
    } catch (Throwable th) {
      responder.sendJson(ProcedureResponse.Code.FAILURE,
                         String.format("Error while processing query %s", th.getMessage()));
    }
  }
}

