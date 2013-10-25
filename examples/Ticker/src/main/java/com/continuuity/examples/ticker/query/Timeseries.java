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
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 */
public class Timeseries extends AbstractProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(Timeseries.class);
  @UseDataSet("tickTimeseries")
  private SimpleTimeseriesTable tickTs;

  @Handle("timeseries")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    long now = TimeUtil.nowInSeconds();
    String ticker = request.getArgument("symbol");
    String dataType = request.getArgument("type");
    dataType = (dataType == null) ? "avg" : dataType;
    long start = TimeUtil.parseTime(now, request.getArgument("start"));
    long end = TimeUtil.parseTime(now, request.getArgument("end"));

    JsonObject output = new JsonObject();
    for (TimeseriesTable.Entry entry : tickTs.read(Bytes.toBytes(ticker), start, end, Bytes.toBytes(dataType))) {
      output.addProperty(String.valueOf(entry.getTimestamp()), Bytes.toFloat(entry.getValue()));
    }
    responder.sendJson(ProcedureResponse.Code.SUCCESS, output);
  }

}
