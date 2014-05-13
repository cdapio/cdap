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

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Flow definition.
 */
public class SimpleWriteAndReadFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("Example flow that writes then reads")
      .withFlowlets()
        .add("source", new KeyValueSource())
        .add("writer", new WriterFlowlet())
        .add("reader", new ReaderFlowlet())
      .connect()
        .fromStream("keyValues").to("source")
        .from("source").to("writer")
        .from("writer").to("reader")
      .build();
  }

  // Flowlets will pass instances of a custom KeyAndValue class

  /**
   * Stores a string key and string value to pass between Flowlets.
   */
  public static class KeyAndValue {
    private String key;
    private String value;

    public KeyAndValue(String key, String value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return this.key;
    }

    public String getValue() {
      return this.value;
    }

    @Override
    public String toString() {
      return key + "=" + value;
    }
  }
}
