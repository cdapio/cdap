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
