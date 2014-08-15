/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.examples.countoddandeven;

import co.cask.cdap.api.annotation.Output;
import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;

/**
 * Based on the whether number is odd or even it pushes (emits) 
 * the number to the appropriate queue.
 */
public class OddOrEven extends AbstractFlowlet {

  @Output("evenNumbers")
  private OutputEmitter<Integer> evenOutput;

  @Output("oddNumbers")
  private OutputEmitter<Integer> oddOutput;

  @ProcessInput
  public void process(Integer number) {
    if (number.intValue() % 2 == 0) {
      evenOutput.emit(number);
    } else {
      oddOutput.emit(number);
    }
  }
}

