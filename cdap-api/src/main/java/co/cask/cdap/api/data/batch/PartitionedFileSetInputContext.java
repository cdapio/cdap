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

package co.cask.cdap.api.data.batch;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;

import java.util.List;

/**
 * Exposes information about the {@link PartitionedFileSet} input configured for this task.
 */
public interface PartitionedFileSetInputContext extends InputContext {

  /**
   * Returns the {@link PartitionKey} of the input configured for this task.
   * This method should be avoided if using CombineFileInputFormat, since in that case, there can be multiple
   * PartitionKeys for this task.
   */
  PartitionKey getInputPartitionKey();

  /**
   * Returns a list of {@link PartitionKey}s of the input configured for this task. There can be multiple PartitionKeys
   * for a single task if using CombineFileInputFormat.
   */
  List<PartitionKey> getInputPartitionKeys();
}
