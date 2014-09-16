/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.TimeseriesTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;

import java.io.IOException;

/**
 * App which creates a dataset using the new Datasets API used for testing
 */
public class AppWithDatasetDuplicate extends AbstractApplication {

  @Override
  public void configure() {
    setName("AppWithDataSetDuplicateName");
    setDescription("Application with Dataset Duplicate Name, but different type, for testing");
    createDataset("myds", TimeseriesTable.class);
    addProcedure(new NoOpProcedure());
  }

  public static final class NoOpProcedure extends AbstractProcedure {
    @Handle("noop")
    public void handle(ProcedureRequest request,
                       ProcedureResponder responder)
      throws IOException {
      responder.sendJson("OK");
    }
  }
}
