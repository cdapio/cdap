/**
 * Copyright 2013-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Dynamic Filter Flowlet.
 */
public class DynamicFilterFlowlet extends AbstractFlowlet {
  private static final Logger LOG =
    LoggerFactory.getLogger (DynamicFilterFlowlet.class);

  private String filterName;
  private String filterRegex;
  private Metrics metric;

  public DynamicFilterFlowlet(String filterName, String filterRegex) {
    super("FilterFlowlet-" + filterName);
    this.filterName = filterName;
    this.filterRegex = filterRegex;
  }

  @Override
  public FlowletSpecification configure() {
    Map<String, String> args = new TreeMap<String, String>();
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
    Map<String, String> args = context.getSpecification().getProperties();
    filterName = args.get("filterName");
    filterRegex = args.get("filterRegex");
    if (filterName == null || filterRegex == null) {
      throw new IllegalArgumentException("Filter name and regex required");
    }
  }

  @Output("counts")
  private OutputEmitter<String> countOutput;

  @ProcessInput("tokens")
  public void process(String token) {
    metric.count("tokens.processed", 1);
    LOG.info("Processing token '" + token + "' against filter with name " +
                   filterName + " and regex " + filterRegex);

    if (Pattern.matches(filterRegex, token)) {
      metric.count("tokens.matched", 1);
      LOG.info("Matched token " + token + " against filter " + filterName);
      countOutput.emit(filterName);
    }

  }
}
