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
import com.continuuity.api.data.dataset.SimpleTimeseriesTable;
import com.continuuity.api.data.dataset.TimeseriesTable;
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
  private SimpleTimeseriesTable tickTs;

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
