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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;

import java.util.Collection;
import java.util.Map;

/**
 * Represents a dataset that is split into partitioned that can be uniquely addressed
 * by meta data. Each partition is a dataset, with arguments to specify input selection
 * and other configuration, and with a value for each of the meta data fields.
 */
public interface Partitioned extends Dataset, InputFormatProvider {

  /**
   * Specifies the type of a meta data field. As of now, only a few primitive types are supported.
   */
  public enum FieldType { STRING, LONG, DATE };

  public Map<String, FieldType> getMetaFields();

  public Collection<Partition> getPartitions(Map<String, Object> metadata);

  public void addPartition(Partition partition);
}
