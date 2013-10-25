package com.continuuity.flows;

import com.continuuity.HayStackApp;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletException;
import com.continuuity.meta.Event;
import com.google.gson.Gson;

import java.util.Map;

/**
 * Persist the log events in datasets.
 */
public class Persister extends AbstractFlowlet {

  private Gson gson;

  @UseDataSet("dataStore")
  private SimpleTimeseriesTable table;

  @ProcessInput
  public void process(Event event){
    String json = gson.toJson(event, Event.class);

    byte[][] tags = new byte[event.getDimensions().size()][];

    int index = 0;
    for (Map.Entry<String, String> entry : event.getDimensions().entrySet()){
      tags[index] = Bytes.toBytes(entry.getValue());
      index++;
    }

    TimeseriesTable.Entry entry = new TimeseriesTable.Entry(HayStackApp.LOG_KEY, Bytes.toBytes(json),
                                                            event.getTimestamp(), tags);
    table.write(entry);
  }

  @Override
  public void initialize(FlowletContext context) throws FlowletException {
    gson = new Gson();
  }
}
