/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.webanalytics;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.StreamEvent;

import java.nio.charset.Charset;

/**
 * UniqueVisitor, a data processing node that processes collected data.
 * In this scenario the input is directly from the stream DataStream itself, the
 * this process function then splits the line using spaces and records the first element
 * into the Dataset UniqueVisitCount. If the element has been recorded before, the
 * Dataset will increment the value of the element.
 */
public class UniqueVisitor extends AbstractFlowlet {

  // Request an instance of UniqueVisitCount Dataset
  @UseDataSet("UniqueVisitCount")
  private UniqueVisitCount table;

  @ProcessInput
  public void process(StreamEvent streamEvent) {
    // Decode the log line as String
    String event = Charset.forName("UTF-8").decode(streamEvent.getBody()).toString();

    // The first entry in the log event is the IP address
    String ip = event.substring(0, event.indexOf(' '));

    // Increments the visit count of a given IP by 1
    table.increment(ip, 1L);
  }
}
