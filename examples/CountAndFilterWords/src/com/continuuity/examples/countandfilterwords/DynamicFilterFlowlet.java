/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
