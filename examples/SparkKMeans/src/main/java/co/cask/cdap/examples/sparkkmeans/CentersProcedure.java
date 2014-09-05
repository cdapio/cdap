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


package co.cask.cdap.examples.sparkkmeans;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static co.cask.cdap.examples.sparkkmeans.SparkKMeansApp.UTF8;

public class CentersProcedure extends AbstractProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(CentersProcedure.class);

  // Annotation indicates that ranks dataset is used in the procedure.
  @UseDataSet("centers")
  private ObjectStore<Double> ranks;

  @Handle("centers")
  public void getCenters(ProcedureRequest request, ProcedureResponder responder)
    throws IOException, InterruptedException {

    //TODO: get all objects

    // Send response with JSON format.
    responder.sendJson(String.valueOf("OK"));
  }
}
