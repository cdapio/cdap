/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.api.dataset.lib.partitioned;

import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.lib.DatasetStatePersistor;

import javax.annotation.Nullable;

/**
 * An implementation of {@link StatePersistor} that uses a {@link DatasetContext} to read and persist state.
 * This is needed to make it possible that the user can define a DatasetStatePersistor, without access to an instance
 * of a DatasetContext (from a Worker, for instance).
 */
public class DelegatingStatePersistor implements StatePersistor {

  private final DatasetContext datasetContext;
  private final DatasetStatePersistor datasetStatePersistor;

  public DelegatingStatePersistor(DatasetContext datasetContext, DatasetStatePersistor datasetStatePersistor) {
    this.datasetContext = datasetContext;
    this.datasetStatePersistor = datasetStatePersistor;
  }

  @Nullable
  @Override
  public byte[] readState() {
    return datasetStatePersistor.readState(datasetContext);
  }

  @Override
  public void persistState(byte[] state) {
    datasetStatePersistor.persistState(datasetContext, state);
  }
}
