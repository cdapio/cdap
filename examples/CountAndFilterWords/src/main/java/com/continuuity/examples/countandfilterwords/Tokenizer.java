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
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.metrics.Metrics;

/**
 * Tokenizer Flowlet.
 */
public class Tokenizer extends AbstractFlowlet {

  @Output("tokens")
  private OutputEmitter<String> tokenOutput;

  @Output("counts")
  private OutputEmitter<String> countOutput;

  private Metrics metric;

  @ProcessInput
  public void process(String line) {
    // Tokenize and emit each token to the filters
    metric.count("lines", 1);
    for (String token : line.split("[ .-]")) {
      metric.count("tokens", 1);
      tokenOutput.emit(token);
      // Also emit to the 'all' counter for each token
      countOutput.emit("all");
    }
  }
}
