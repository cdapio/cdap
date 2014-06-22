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
  private TimeseriesTable tickTs;

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
