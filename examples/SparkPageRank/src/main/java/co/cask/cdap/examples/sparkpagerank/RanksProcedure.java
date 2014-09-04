/*
 * Copyright 2014 Cask Data, Inc.
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


package co.cask.cdap.examples.sparkpagerank;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RanksProcedure extends AbstractProcedure {
  // Annotation indicates that countTable dataset is used in the procedure.
  @UseDataSet("ranks")
  private ObjectStore<Double> ranks;

  @Handle("rank")
  public void getRank(ProcedureRequest request, ProcedureResponder responder)
    throws IOException, InterruptedException {

    // Get the end time of the time range from the query parameters. By default, end time is now.
    String url = request.getArgument("url");

    Map<String, Double> hourCount = new HashMap<String, Double>();

    hourCount.put(url, ranks.read(url.getBytes()));

    // Send response with JSON format.
    responder.sendJson(hourCount);
  }
}
