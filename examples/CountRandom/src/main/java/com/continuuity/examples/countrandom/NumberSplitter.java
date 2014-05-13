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
package com.continuuity.examples.countrandom;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

/**
 * Number splitter Flowlet {@code NumberSplitter}.
 */
public class NumberSplitter extends AbstractFlowlet {
  private OutputEmitter<Integer> output;

  @ProcessInput
  public void process(Integer number)  {
    output.emit(new Integer(number % 10000));
    output.emit(new Integer(number % 1000));
    output.emit(new Integer(number % 100));
    output.emit(new Integer(number % 10));
  }
}

