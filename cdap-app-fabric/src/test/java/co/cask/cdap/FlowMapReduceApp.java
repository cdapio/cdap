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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.KeyValueTable;

/**
 * App with Flow and MapReduce. used for testing MDS.
 */
public class FlowMapReduceApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("App");
    setDescription("Application which has everything");
    addStream(new Stream("stream"));
    createDataset("kvt", KeyValueTable.class);
    addFlow(new AllProgramsApp.NoOpFlow());
    addMapReduce(new AllProgramsApp.NoOpMR());
  }
}
