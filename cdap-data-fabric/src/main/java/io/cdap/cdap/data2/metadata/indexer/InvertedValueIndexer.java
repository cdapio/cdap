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
import co.cask.cdap.data2.metadata.dataset.SortInfo;

import java.util.Collections;
import java.util.Set;

/**
 * An {@link Indexer} that returns lexicographically inverted indexes. The indexes returned have each character's
 * ASCII value subtracted from the maximum ASCII value of a character (127).
 */
public class InvertedValueIndexer implements Indexer {
  @Override
  public Set<String> getIndexes(MetadataEntry entry) {
    return Collections.singleton(lexInvert(entry.getValue()));
  }

  private String lexInvert(String input) {
    StringBuilder reversed = new StringBuilder();
    for (char c : input.toCharArray()) {
      int ascii = (int) c;
      // 127 is the max ASCII value
      int inverted = 127 - ascii;
      reversed.append((char) inverted);
    }
    return reversed.toString();
  }

  @Override
  public SortInfo.SortOrder getSortOrder() {
    return SortInfo.SortOrder.DESC;
  }
}
