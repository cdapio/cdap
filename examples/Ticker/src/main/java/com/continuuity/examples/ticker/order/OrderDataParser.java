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

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.InputContext;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Parses input in the form of a csv of key value pairs, where keys and values are separated by a '=' sign.
 * It is expected that every input will have a timestamp.  For example:
 *
 * timestamp=20131007T103055,symbol=AAPL,exchange=NYSE,broker=JPMC,currency=USD,payload=iVBORw0KGgo
 */
public class OrderDataParser extends AbstractFlowlet {
  private static final Logger LOG = LoggerFactory.getLogger(OrderDataParser.class);
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss");

  private OutputEmitter<OrderRecord> output;
  private Metrics metrics;

  /**
   * Processes new order data records.  Order data records are expected to be a CSV of keyvalue pairs,
   * with a '=' delimiting keys and values.  For example:
   * timestamp=20131007T103055,symbol=AAPL,exchange=NYSE,broker=JPMC,currency=USD,payload=iVBORw0KGgo
   */
  @ProcessInput
  public void process(StreamEvent event, InputContext context) throws CharacterCodingException {
    ByteBuffer buf = event.getBody();
    if (buf != null) {
      String body = Charsets.UTF_8.newDecoder().decode(buf).toString();
      Map<String, String> fields = parseRecord(body);
      if (fields.containsKey(OrderRecord.TIMESTAMP_KEY)) {
        try {
          Long timestamp = dateFormat.parse(fields.get(OrderRecord.TIMESTAMP_KEY)).getTime();
          fields.put(OrderRecord.TIMESTAMP_KEY, timestamp.toString());
        } catch (ParseException pe) {
          LOG.warn("Invalid timestamp value: " + fields.get(OrderRecord.TIMESTAMP_KEY));
          metrics.count("events.badInput", 1);
          return;
        }
      }
      // Compute the ID as a hash of the full fields
      byte[] id = Hashing.md5().hashString(body).asBytes();
      try {
        OrderRecord record = new OrderRecord(id, fields);
        LOG.debug("Processing order data for timestamp {}", record.getTimestamp());

        output.emit(record);
        metrics.count("events.input", 1);
      } catch (IllegalArgumentException ie) {
        LOG.warn("Bad input record: {}", ie.getMessage());
        metrics.count("events.badInput", 1);
      }
    } else {
      metrics.count("events.nullcount", 1);
    }

  }

  private Map<String, String> parseRecord(String body) {
    String[] keyValues = body.split(",");
    Map<String, String> fields = Maps.newHashMapWithExpectedSize(keyValues.length);
    for (String kv : keyValues) {
      String[] pair = kv.split("=", 2);
      if (pair.length != 2) {
        LOG.error("Invalid format for field: {}", kv);
        metrics.count("error.badInput", 1);
      } else {
        fields.put(pair[0], pair[1]);
      }
    }
    return fields;
  }
}
