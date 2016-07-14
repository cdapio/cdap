/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.cask360;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;

/**
 * This is an example application for using the {@link Cask360Table}.
 * <p>
 * The app contains a single {@link Cask360Table} with a {@link Cask360Service}
 * to access that table.
 * <p>
 * There is also a stream connected to a flow which takes CSV or JSON formatted
 * events and writes them to the table.
 * <p>
 * SQL queries can also be run on the table.
 */
@SuppressWarnings("rawtypes")
public class Cask360App extends AbstractApplication {

  /** Name of the Application */
  public static final String APP_NAME = "Cask360App";

  /** Description of the Application */
  public static final String APP_DESC = "Cask360 Application";

  /** Name of the Table */
  public static final String TABLE_NAME = "customer360";

  /** Name of the Service */
  public static final String SERVICE_NAME = "Cask360Service";

  /** Description of the Service */
  public static final String SERVICE_DESC = "Cask360 Service";

  /** Name of the Stream */
  public static final String STREAM_NAME = "Cask360Stream";

  /** Name of the Flow */
  public static final String FLOW_NAME = "Cask360Flow";

  /** Description of the Flow */
  public static final String FLOW_DESC = "Cask360 Flow";

  @Override
  @SuppressWarnings("unchecked")
  public void configure() {
    setName(APP_NAME);
    setDescription(APP_DESC);
    createDataset(TABLE_NAME, Cask360Table.class);
    addService(new Cask360Service(SERVICE_NAME, SERVICE_DESC, TABLE_NAME));
    addStream(STREAM_NAME);
    addFlow(new Cask360Flow(FLOW_NAME, FLOW_DESC, STREAM_NAME, TABLE_NAME));
  }
}
