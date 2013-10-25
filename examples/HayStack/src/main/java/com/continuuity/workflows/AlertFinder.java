package com.continuuity.workflows;

import com.continuuity.HayStackApp;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.batch.AbstractMapReduce;
import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.workflow.AbstractWorkflowAction;
import com.continuuity.api.workflow.WorkflowContext;
import com.continuuity.data.common.DataPoint;
import com.continuuity.data.cube.Cube;
import com.continuuity.data.cube.CubeQuery;
import com.continuuity.data.cube.CubeResult;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

//TODO: Replace with mapreduce when WorkflowAction can have access to datasets

/**
 * Alert Finder
 */
public class AlertFinder extends AbstractMapReduce {

  @UseDataSet("dataStore")
  private SimpleTimeseriesTable table;

  @UseDataSet("logAnalytics")
  private Cube aggregator;

  private static Logger LOG = LoggerFactory.getLogger(AlertFinder.class);
  private long start;
  private long end;
  private int warnThreshold;
  private int errorThreshold;
  private Gson gson;


  @Override
  public void initialize(WorkflowContext context) throws Exception {
    long now = System.currentTimeMillis();

    String startTime = context.getRuntimeArguments().get("start");
    String endTime = context.getRuntimeArguments().get("end");
    String warn = context.getRuntimeArguments().get("warnThreshold");
    String error = context.getRuntimeArguments().get("errorThreshold");

    start = startTime == null? now - 12 * 60 * 60 * 1000L : Long.parseLong(startTime);
    end = endTime == null? now : Long.parseLong(endTime);

    warnThreshold = warn == null ? 5: Integer.parseInt(warn);
    errorThreshold = error == null ? 5: Integer.parseInt(error);

    gson = new Gson();
  }

  @Override
  public void run() {
    List<Alert> alertsWarn = getAlerts(start, end, Alert.Type.WARN, warnThreshold);
    List<Alert> alertsErr = getAlerts(start, end, Alert.Type.ERROR, errorThreshold);

    for (Alert alertWarn : alertsWarn) {
      String json = gson.toJson(alertWarn, Alert.class);
      TimeseriesTable.Entry entry  = new TimeseriesTable.Entry(HayStackApp.ALERT_KEY_WARN,
                                                               Bytes.toBytes(json), alertWarn.getTimestamp());
      table.write(entry);
    }

    for (Alert alertErr : alertsErr) {
      String json = gson.toJson(alertErr, Alert.class);
      TimeseriesTable.Entry entry  = new TimeseriesTable.Entry(HayStackApp.ALERT_KEY_ERR,
                                                               Bytes.toBytes(json), alertErr.getTimestamp());
      table.write(entry);
    }

  }

  private List<Alert> getAlerts(long start, long end, Alert.Type type, int threshold) {
    List<DataPoint> dataPoints = getResultFromCube(start, end, "level", type.toString());
    List<Alert> alerts = Lists.newArrayList();
    for (DataPoint data : dataPoints) {
      if (data.getCount() > threshold) {
        String message  = String.format("Number of %s messages: %s greater than threshold: %d at %d",
                                        type.toString(), data.getCount(), warnThreshold, data.getTimestamp());
        LOG.info("Triggered alert message: {}", message);
        Alert alert = new Alert(data.getTimestamp(), type, message);
        alerts.add(alert);
      }
    }
    return alerts;
  }

  private List<DataPoint> getResultFromCube(long start, long end, String dimension, String value) {
    CubeQuery query = aggregator.buildQuery()
      .select()
      .count()
      .dimension(dimension)
      .value(value)
      .group()
      .byInterval(HayStackApp.AGGREGATION_INTERVAL, HayStackApp.AGGREGATION_TIMEUNIT, start, end)
      .fillZeros()
      .build();

    CubeResult result = aggregator.read(query);
    return result.getDataPoints();
  }

  @Override
  public MapReduceSpecification configure() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
