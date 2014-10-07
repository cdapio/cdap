/*
 * *
 *  Copyright © 2014 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 * /
 */

package co.cask.cdap.test.app;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;

import java.io.IOException;

/**
 * App that uses fake dataset (custom dataset)
 */
public class AppWithDatasetVersion1 extends AbstractApplication {

  public static final String NAME = "DummyApp1";
  public static final String DS_NAME = "dummyds";

  @Override
  public void configure() {
    setName(NAME);
    addProcedure(new NoOpProcedure());
    createDataset(DS_NAME, FakeDataset.class);
  }

  public static final class NoOpProcedure extends AbstractProcedure {
    @Handle("ping")
    public void ping(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, "Hello");
    }
  }
}
