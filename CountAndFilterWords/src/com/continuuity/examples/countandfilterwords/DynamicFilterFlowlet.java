package com.continuuity.examples.countandfilterwords;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class DynamicFilterFlowlet extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(DynamicFilterFlowlet.class);

  private String filterName;
  private String filterRegex;

  public DynamicFilterFlowlet(String filterName, String filterRegex) {
    this.filterName = filterName;
    this.filterRegex = filterRegex;
  }

  @Override
  public FlowletSpecification configure() {
    Map<String,String> args = new TreeMap<String,String>();
    args.put("filterName", filterName);
    args.put("filterRegex", filterRegex);
    return FlowletSpecification.Builder.with()
        .setName("DynamicFilterFlowlet")
        .setDescription("Dynamic Filter Flowlet")
        .withArguments(args)
        .build();
  }

  @Override
  public void initialize(FlowletContext context) {
    Map<String,String> args = context.getSpecification().getArguments();
    filterName = args.get("filterName");
    filterRegex = args.get("filterRegex");
    if (filterName == null || filterRegex == null)
      throw new IllegalArgumentException("Filter name and regex required");
  }

  @Output("counts")
  private OutputEmitter<String> countOutput;

  @ProcessInput("tokens")
  public void process(String token) {
    LOG.debug("Processing token '" + token + "' against filter with name " +
        filterName + " and regex " + filterRegex);
    
    if (Pattern.matches(filterRegex, token)) {
      LOG.debug("Matched token " + token + " against filter " + filterName);
      countOutput.emit(filterName);
    }

  }
}
