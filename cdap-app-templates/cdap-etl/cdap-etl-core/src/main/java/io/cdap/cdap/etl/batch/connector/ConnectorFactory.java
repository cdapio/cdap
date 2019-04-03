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

import co.cask.cdap.api.data.schema.Schema;

import javax.annotation.Nullable;

/**
 * Creates ConnectorSources and ConnectorSinks
 */
public interface ConnectorFactory {

  /**
   * Create a connector source
   *
   * @param datasetName the name of the connector dataset
   * @return a new connector source
   */
  ConnectorSource createSource(String datasetName);

  /**
   * Create a connector sink
   *
   * @param datasetName the name of the connector dataset
   * @param phaseName the name of the phase that will be writing to the dataset
   * @return a new connector sink
   */
  ConnectorSink createSink(String datasetName, String phaseName);
}
