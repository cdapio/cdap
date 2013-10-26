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
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Put;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.examples.ticker.data.MultiIndexedTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Saves order data in a multi indexed table.
 */
public class OrderDataSaver extends AbstractFlowlet {
  public static final byte[] TIMESTAMP_COL = Bytes.toBytes("ts");
  public static final byte[] PAYLOAD_COL = Bytes.toBytes("p");

  private static final Logger LOG = LoggerFactory.getLogger(OrderDataSaver.class);

  @UseDataSet("orderIndex")
  private MultiIndexedTable orderTable;

  @ProcessInput
  public void process(OrderRecord order) {
    Put put = new Put(order.getId());
    put.add(TIMESTAMP_COL, order.getTimestamp());
    put.add(PAYLOAD_COL, order.getPayload());
    for (Map.Entry<String, String> field : order.getFields().entrySet()) {
      put.add(Bytes.toBytes(field.getKey()), Bytes.toBytes(field.getValue()));
    }
    orderTable.put(put);
  }
}
