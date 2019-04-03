/*
 * Copyright © 2018-2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.data2.metadata.indexer;

import io.cdap.cdap.data2.metadata.dataset.MetadataEntry;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.metadata.MetadataConstants;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link DefaultValueIndexer}.
 */
public class DefaultValueIndexerTest {
  private static final Indexer indexer = new DefaultValueIndexer();

  @Test
  public void testSimpleProperty() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), "key", "val");
    Set<String> expected = new HashSet<>();
    expected.add("val");
    expected.add("key:val");
    expected.add("properties:key");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }

  @Test
  public void testSingleSplitProperty() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), "key", "foo bar");
    Set<String> expected = new HashSet<>();
    // CDAP-13629 - seems odd 'foo bar' is generated here, but not for a single tag 'foo bar'
    expected.add("foo bar");
    expected.add("foo");
    expected.add("bar");
    expected.add("key:foo bar");
    expected.add("key:foo");
    expected.add("key:bar");
    expected.add("properties:key");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }

  @Test
  public void testSingleSimpleTags() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), MetadataConstants.TAGS_KEY, "tag");
    Set<String> expected = new HashSet<>();
    expected.add("tag");
    expected.add(MetadataConstants.TAGS_KEY + ":tag");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }

  @Test
  public void testSingleSplitTags() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), MetadataConstants.TAGS_KEY, "foo bar");
    Set<String> expected = new HashSet<>();
    expected.add("foo");
    expected.add("bar");
    expected.add(MetadataConstants.TAGS_KEY + ":foo");
    expected.add(MetadataConstants.TAGS_KEY + ":bar");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }

  @Test
  public void testMultipleTags() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), MetadataConstants.TAGS_KEY, "t1,t2,t3");
    Set<String> expected = new HashSet<>();
    expected.add("t1");
    expected.add("t2");
    expected.add("t3");
    expected.add(MetadataConstants.TAGS_KEY + ":t1");
    expected.add(MetadataConstants.TAGS_KEY + ":t2");
    expected.add(MetadataConstants.TAGS_KEY + ":t3");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }

  @Test
  public void testMultipleSplitTags() {
    MetadataEntry entry = new MetadataEntry(NamespaceId.DEFAULT.app("a"), MetadataConstants.TAGS_KEY, "foo,bar baz");
    Set<String> expected = new HashSet<>();
    expected.add("foo");
    expected.add("bar");
    expected.add("baz");
    expected.add(MetadataConstants.TAGS_KEY + ":foo");
    expected.add(MetadataConstants.TAGS_KEY + ":bar");
    expected.add(MetadataConstants.TAGS_KEY + ":baz");
    Assert.assertEquals(expected, indexer.getIndexes(entry));
  }
}
