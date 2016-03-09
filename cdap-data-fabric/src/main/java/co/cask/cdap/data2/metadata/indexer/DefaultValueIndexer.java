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

import co.cask.cdap.data2.metadata.dataset.MetadataDataset;
import co.cask.cdap.data2.metadata.dataset.MetadataEntry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link Indexer} for {@link MetadataEntry}
 */
public class DefaultValueIndexer implements Indexer {
  private static final Pattern VALUE_SPLIT_PATTERN = Pattern.compile("[-_:,\\s]+");
  private static final Pattern TAGS_SEPARATOR_PATTERN = Pattern.compile("[,\\s]+");

  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    Set<String> valueIndexes = new HashSet<>();
    if (entry.getKey().equalsIgnoreCase(MetadataDataset.TAGS_KEY)) {
      // if the entry is tag then each tag is an index
      valueIndexes.addAll(Arrays.asList(TAGS_SEPARATOR_PATTERN.split(entry.getValue())));
    } else {
      // for key value the complete value is an index
      valueIndexes.add(entry.getValue());
    }
    Set<String> indexes = new HashSet<>();
    for (String index : valueIndexes) {
      // split all value indexes on the VALUE_SPLIT_PATTERN
      indexes.addAll(Arrays.asList(VALUE_SPLIT_PATTERN.split(index)));
    }
    // add all value indexes too
    indexes.addAll(valueIndexes);
    return indexes;
  }
}
