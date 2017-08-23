/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.connector;

/**
 * Creates Multi connector sources and sinks.
 */
public class MultiConnectorFactory implements ConnectorFactory {

  @Override
  public ConnectorSource createSource(String datasetName) {
    return new MultiConnectorSource(datasetName, null);
  }

  @Override
  public ConnectorSink createSink(String datasetName, String phaseName) {
    return new MultiConnectorSink(datasetName, phaseName);
  }
}
