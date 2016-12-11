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
import co.cask.cdap.proto.id.NamespaceId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link InvertedValueIndexer}.
 */
public class InvertedValueIndexerTest {
  private final Indexer indexer = new InvertedValueIndexer();
  private final NamespaceId ns = new NamespaceId("ns");

  @Test
  public void testSimple() {
    List<String> inputs = ImmutableList.of(
      "134342", "435ert5", "trdfrw", "_bfcfd", "r34_r3", "cgsdfgs)dfd", "gfsgfd2345245234", "dfsgs"
    );
    // expected is reverse sorted input
    List<String> expected = new ArrayList<>(inputs);
    Collections.sort(expected, Collections.<String>reverseOrder());
    List<String> invertedIndexes = new ArrayList<>();
    for (String input : inputs) {
      invertedIndexes.add(Iterables.getOnlyElement(indexer.getIndexes(new MetadataEntry(ns, "dontcare", input))));
    }
    // inverted indexes sorted in ascending order
    Collections.sort(invertedIndexes);
    for (int i = 0; i < invertedIndexes.size(); i++) {
      String invertedIndex = invertedIndexes.get(i);
      String original = Iterables.getOnlyElement(indexer.getIndexes(new MetadataEntry(ns, "dontcare", invertedIndex)));
      Assert.assertEquals(expected.get(i), original);
    }
  }
}
