/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.indexer;

import io.cdap.cdap.data2.metadata.dataset.MetadataEntry;
import io.cdap.cdap.data2.metadata.dataset.SortInfo;
import io.cdap.cdap.spi.metadata.MetadataConstants;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Default {@link Indexer} for {@link MetadataEntry}.
 */
public class DefaultValueIndexer implements Indexer {
  private static final Pattern VALUE_SPLIT_PATTERN = Pattern.compile("[-_:,\\s]+");
  private static final Pattern TAGS_SEPARATOR_PATTERN = Pattern.compile("[,\\s]+");

  /**
   * Generates a set of tokens based on the metadata values. Splits values on whitespace, '-', '_', ':', and ',' to
   * generate multiple tokens. Also generates an additional token that has the metadata key prefixed to it.
   * Also adds a token that allows searching the property name, that is, properties:key, except if the key is "tags".
   *
   * For example, when given property 'owner'='foo bar', six tokens will be generated:
   *
   * 'foo bar', 'foo', 'bar', 'owner:foo bar', 'owner:foo', 'owner:bar', and 'properties:owner'.
   *
   * If 'tags'='foo,bar baz' is given, six tokens will be generated:
   *
   * 'foo', 'bar', 'baz', 'tags:foo', 'tags:bar', 'tags:baz'
   *
   * TODO: (CDAP-13629) be consistent with properties and tags on the 'foo bar' case.
   *
   * @param entry the {@link MetadataEntry} for which indexes needs to be created
   * @return split and prefixed index values
   */
  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    Set<String> valueIndexes = new HashSet<>();
    if (entry.getKey().equalsIgnoreCase(MetadataConstants.TAGS_KEY)) {
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
    // store the index with key of the metadata, so that we allow searches of the form [key]:[value]
    return addKeyValueIndexes(entry.getKey(), indexes);
  }

  @Override
  public SortInfo.SortOrder getSortOrder() {
    return SortInfo.SortOrder.WEIGHTED;
  }

  private Set<String> addKeyValueIndexes(String key, Set<String> indexes) {
    Set<String> indexesWithKeyValue = new HashSet<>(indexes);
    for (String index : indexes) {
      indexesWithKeyValue.add(key + MetadataConstants.KEYVALUE_SEPARATOR + index);
    }
    if (!key.equalsIgnoreCase(MetadataConstants.TAGS_KEY)) {
      indexesWithKeyValue.add(MetadataConstants.PROPERTIES_KEY + MetadataConstants.KEYVALUE_SEPARATOR + key);
    }
    return indexesWithKeyValue;
  }
}
