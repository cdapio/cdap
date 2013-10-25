package com.continuuity.flows;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.data.cube.Cube;
import com.continuuity.data.cube.Fact;
import com.continuuity.meta.Event;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;


/**
 * Aggregator of events.
 */
public class Aggregator extends AbstractFlowlet {

  @UseDataSet("logAnalytics")
  private Cube aggregator;

  @ProcessInput
  public void process(Event event){

    Fact.ValueAdding valueAdding = aggregator.buildFact()
                                             .count(1L)
                                             .time(event.getTimestamp());

    for(Map.Entry<String, String> entry : event.getDimensions().entrySet()) {
      valueAdding = valueAdding.value(entry.getKey(), entry.getValue());
    }

    Fact fact = valueAdding.build();
    aggregator.write(fact);
  }

}
