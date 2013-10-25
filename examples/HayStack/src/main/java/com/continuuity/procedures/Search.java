package com.continuuity.procedures;

import com.continuuity.HayStackApp;
import com.continuuity.api.Resources;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.meta.Event;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Search related queries.
 */
public class Search extends AbstractProcedure {

  private static Gson gson = new Gson();
  private static Logger LOG = LoggerFactory.getLogger(Search.class);

  @UseDataSet("dataStore")
  SimpleTimeseriesTable table;

  private long TIME_24_HRS_MILLIS = 24 * 60 * 60 * 1000L;

  @Handle("timeBoundSearch")
  public void timeBoundSearch(ProcedureRequest request, ProcedureResponder responder) throws IOException {

    String startTime = request.getArgument("start");
    String endTime = request.getArgument("end");

    List<String> dimensions = Lists.newArrayList();

    String hostname = request.getArgument("hostname");
    String component = request.getArgument("component");
    String level = request.getArgument("level");
    String className = request.getArgument("class");

    if (hostname != null) {
      dimensions.add(hostname);
    }

    if (component != null) {
      dimensions.add(component);
    }

    if (level != null) {
      dimensions.add(level);
    }

    if (className != null) {
      dimensions.add(className);
    }

    long end = System.currentTimeMillis();
    long start = System.currentTimeMillis() - TIME_24_HRS_MILLIS;

    if (startTime != null) {
      start = Long.parseLong(startTime);
    }

    if (endTime != null) {
      end = Long.parseLong(endTime);
    }

    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.CLIENT_ERROR, "Start time cannot be greater than end");
      return;
    }

    List<TimeseriesTable.Entry> entries;

    if (dimensions.isEmpty()) {
      entries = table.read(HayStackApp.LOG_KEY, start, end);
    } else {
      byte[][] tags = new byte[dimensions.size()][];
      for (int i = 0; i < dimensions.size(); i++) {
        tags[i] = Bytes.toBytes(dimensions.get(i));
      }
      entries = table.read(HayStackApp.LOG_KEY, start, end, tags);
    }

    List<Event> result = Lists.newArrayList();
    for (TimeseriesTable.Entry entry : entries){
      byte[] value = entry.getValue();
      String json = Bytes.toString(value);
      LOG.debug("result json {}", json);
      Event event = gson.fromJson(json, Event.class);
      result.add(event);
    }
    responder.sendJson(ProcedureResponse.Code.SUCCESS, result);
  }

  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName("Search")
      .setDescription("Search procedures")
      .withResources(new Resources(256))
      .build();
  }

}
