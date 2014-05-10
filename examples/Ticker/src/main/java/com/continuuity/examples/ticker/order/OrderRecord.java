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
package com.continuuity.examples.ticker.order;

import com.continuuity.api.common.Bytes;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Record of a stock order with arbitrary key value pairs attached to it, a payload, and a timestamp.
 */
public class OrderRecord {
  public static final String PAYLOAD_KEY = "payload";
  public static final String TIMESTAMP_KEY = "timestamp";

  private byte[] id;
  private byte[] payload;
  private long timestamp;

  private Map<String, String> fields;

  public OrderRecord(byte[] id, Map<String, String> rawFields) {
    this.id = id;
    this.fields = Maps.newHashMapWithExpectedSize(rawFields.size());
    for (Map.Entry<String, String> e : rawFields.entrySet()) {
      if (PAYLOAD_KEY.equals(e.getKey())) {
        this.payload = Bytes.toBytes(e.getValue());
      } else if (TIMESTAMP_KEY.equals(e.getKey())) {
        this.timestamp = Long.parseLong(e.getValue());
      } else {
        fields.put(e.getKey(), e.getValue());
      }
    }
  }

  public byte[] getId() {
    return this.id;
  }

  public byte[] getPayload() {
    return this.payload;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public Map<String, String> getFields() {
    return this.fields;
  }
}
