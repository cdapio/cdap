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
package com.continuuity.examples.ticker.query;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.lib.TimeseriesTable;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.examples.ticker.TimeUtil;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;

/**
 * Given a start time, end time, and ticker symbol, calculate several summary stats.
 * These stats include the mean, median, and standard deviation of the stock price during the time interval,
 * the most trades that happened in one day and the day it happened on, the lowest price during the time
 * interval and the day it happened on, and the highest price during the time interval and the day it happened on.
 * Start and end times can be specified as timestamps in seconds, or as 'now' for the current time, or
 * as 'now-X[units]' where X is some number of units, and the unit can be 's' for seconds, 'm' for minutes,
 * 'h' for hours, and 'd' for days.
 */
public class Summary extends AbstractProcedure {
  @UseDataSet("tickTimeseries")
  private TimeseriesTable tickTs;

  private static final DecimalFormat df = new DecimalFormat("#.##");

  @Handle("summary")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    long now = TimeUtil.nowInSeconds();
    String ticker = request.getArgument("symbol");
    long start = TimeUtil.parseTime(now, request.getArgument("start"));
    long end = TimeUtil.parseTime(now, request.getArgument("end"));
    if (start > end) {
      responder.sendJson(ProcedureResponse.Code.FAILURE, "start cannot be greater than end");
    }

    JsonObject output = new JsonObject();
    addQuantityStats(ticker, start, end, output);
    addPriceStats(ticker, start, end, output);
    responder.sendJson(ProcedureResponse.Code.SUCCESS, output);
  }

  private void addQuantityStats(String ticker, long start, long end, JsonObject output) {
    Multiset<String> dayTrades = HashMultiset.create();
    for (TimeseriesTable.Entry entry : tickTs.read(Bytes.toBytes(ticker), start, end, Bytes.toBytes("quantity"))) {
      String date = TimeUtil.timestampToDate(entry.getTimestamp());
      dayTrades.add(date, Bytes.toInt(entry.getValue()));
    }

    int totalQuantity = 0;
    int maxQuantity = 0;
    String busiestDay = "";
    for (Multiset.Entry<String> entry : dayTrades.entrySet()) {
      String date = entry.getElement();
      int count = entry.getCount();
      totalQuantity += count;
      if (count > maxQuantity) {
        maxQuantity = count;
        busiestDay = date;
      }
    }
    output.addProperty("mostTradesOn", busiestDay);
    output.addProperty("mostTrades", maxQuantity);
    output.addProperty("totalTraded", totalQuantity);
  }

  private void addPriceStats(String ticker, long start, long end, JsonObject output) {
    float lowest = Float.MAX_VALUE;
    String lowestDate = "";
    float highest = Float.MIN_VALUE;
    String highestDate = "";
    List<Float> prices = Lists.newLinkedList();
    float totalPrice = 0;
    float numTrades = 0;
    for (TimeseriesTable.Entry entry : tickTs.read(Bytes.toBytes(ticker), start, end, Bytes.toBytes("avg"))) {
      String date = TimeUtil.timestampToDate(entry.getTimestamp());
      float val = Bytes.toFloat(entry.getValue());
      totalPrice += val;
      numTrades++;
      prices.add(val);
      if (val < lowest) {
        lowest = val;
        lowestDate = date;
      }
      if (val > highest) {
        highest = val;
        highestDate = date;
      }
    }

    double meanPrice = totalPrice / numTrades;
    double variance = 0;
    for (Float price : prices) {
      variance += (price - meanPrice) * (price - meanPrice);
    }
    double stddev = Math.sqrt(variance / numTrades);
    Collections.sort(prices);

    output.addProperty("medianPrice", df.format(prices.get(prices.size() / 2)));
    output.addProperty("meanPrice", df.format(meanPrice));
    output.addProperty("stdDevPrice", df.format(stddev));
    output.addProperty("highestPrice", df.format(highest));
    output.addProperty("highestOn", highestDate);
    output.addProperty("lowestPrice", df.format(lowest));
    output.addProperty("lowestOn", lowestDate);
  }

}
