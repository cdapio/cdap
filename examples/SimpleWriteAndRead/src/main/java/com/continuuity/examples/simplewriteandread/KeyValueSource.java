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
package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.examples.simplewriteandread.SimpleWriteAndReadFlow.KeyAndValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeyValue source.
 */
public class KeyValueSource extends AbstractFlowlet {
  private static final Logger loger = LoggerFactory.getLogger(KeyValueSource.class);
  private OutputEmitter<KeyAndValue> output;

  public KeyValueSource() {
    super("source");
  }

  @ProcessInput
  public void process(StreamEvent event) throws IllegalArgumentException {
    loger.debug(this.getContext().getName() + ": Received event " + event);

    String text = Bytes.toString(Bytes.toBytes(event.getBody()));

    String[] fields = text.split("=");

    if (fields.length != 2) {
      throw new IllegalArgumentException("Input event must be in the form " +
                                           "'key=value', received '" + text + "'");
    }

    output.emit(new KeyAndValue(fields[0], fields[1]));
  }
}
