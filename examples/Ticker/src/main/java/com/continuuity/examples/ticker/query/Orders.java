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

package com.continuuity.examples.ticker.query;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Row;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.examples.ticker.TimeUtil;
import com.continuuity.examples.ticker.data.MultiIndexedTable;
import com.continuuity.examples.ticker.order.OrderDataSaver;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sun.jersey.core.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Query for stock orders that match the set of filters passed in.
 * Start time, end time, and any possible key value pair associated with the order can be passed in.
 * Start and end times can be specified as timestamps in seconds, or as 'now' for the current time, or
 * as 'now-X[units]' where X is some number of units, and the unit can be 's' for seconds, 'm' for minutes,
 * 'h' for hours, and 'd' for days.
 * For example, start=now-1d, end=now, symbol=AAPL, exchange=NASDAQ, currency=USD will return back all orders
 * in the past day traded on the NASDAQ exchange in US dollars for Apple stock.
 */
public class Orders extends AbstractProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(Orders.class);

  @UseDataSet("orderIndex")
  private MultiIndexedTable orderIndex;

  @Handle("orders")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    long nowInSeconds = TimeUtil.nowInSeconds();
    long startTime = 0;
    long endTime = Long.MAX_VALUE - 1;

    StringBuilder paramString = new StringBuilder();
    Map<byte[], byte[]> queryParams = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (Map.Entry<String, String> arg : request.getArguments().entrySet()) {
      if ("start".equals(arg.getKey())) {
        startTime = TimeUtil.parseTime(nowInSeconds, arg.getValue()) * 1000;
      } else if ("end".equals(arg.getKey())) {
        endTime = TimeUtil.parseTime(nowInSeconds, arg.getValue()) * 1000;
      } else {
        if (paramString.length() > 0) {
          paramString.append(", ");
        }
        paramString.append(arg.getKey()).append("=").append(arg.getValue());
        queryParams.put(Bytes.toBytes(arg.getKey()), Bytes.toBytes(arg.getValue()));
      }
    }

    LOG.info("Querying order index: startTime=" + startTime + ", endTime=" + endTime + ", params=[" +
             paramString.toString() + "]");
    List<Row> results = orderIndex.readBy(queryParams, startTime, endTime);
    LOG.info("Got " + results.size() + " results");

    JsonObject output = new JsonObject();
    JsonArray items = new JsonArray();
    for (Row r : results) {
      items.add(toJson(r));
    }
    output.add("results", items);

    responder.sendJson(ProcedureResponse.Code.SUCCESS, output);
  }

  private JsonObject toJson(Row row) {
    JsonObject obj = new JsonObject();
    obj.addProperty("id", Bytes.toStringBinary(row.getRow()));
    Map<byte[], byte[]> fields = row.getColumns();
    for (Map.Entry<byte[], byte[]> e : fields.entrySet()) {
      if (Bytes.equals(OrderDataSaver.TIMESTAMP_COL, e.getKey())) {
        obj.addProperty("timestamp", Bytes.toLong(e.getValue()));
      } else if (Bytes.equals(OrderDataSaver.PAYLOAD_COL, e.getKey())) {
        obj.addProperty("payload", Bytes.toString(Base64.encode(e.getValue())));
      } else {
        obj.addProperty(Bytes.toString(e.getKey()), Bytes.toString(e.getValue()));
      }
    }
    return obj;
  }
}
