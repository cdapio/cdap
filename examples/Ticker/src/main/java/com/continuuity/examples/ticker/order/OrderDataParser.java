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
   * Processes new order data records.  Order data records are expected to be a csv of keyvalue pairs,
   * with a '=' delimiting keys and values.  For ex:
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
      // compute the ID as a hash of the full fields
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
