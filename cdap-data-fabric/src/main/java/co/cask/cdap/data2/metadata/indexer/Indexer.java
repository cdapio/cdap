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

package co.cask.cdap.data2.metadata.indexer;

import co.cask.cdap.data2.metadata.dataset.MetadataEntry;

import java.util.Set;

/**
 * Indexer for indexing {@link MetadataEntry}
 */
public interface Indexer {

  /**
   * Creates the indexes for the {@link MetadataEntry}
   *
   * @param entry the {@link MetadataEntry} for which indexes needs to be created
   * @return a {@link Set Set&lt;String&gt;} containing indexes for the given {@link MetadataEntry}
   */
  Set<String> getIndexes(MetadataEntry entry);
}
