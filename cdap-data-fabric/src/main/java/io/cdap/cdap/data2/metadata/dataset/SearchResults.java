/*
 * Copyright 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.metadata.dataset;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Represents a list of {@link MetadataEntry} that match a search query in the metadata index, along with a
 * list of cursors to start subsequent searches from.
 */
public class SearchResults {
  private final List<MetadataResultEntry> results;
  private final List<String> cursors;


  SearchResults(List<MetadataResultEntry> results, List<String> cursors) {
    this.results = results;
    this.cursors = cursors;
  }

  public List<MetadataResultEntry> getResults() {
    return results;
  }

  public List<MetadataEntry> getEntries() {
    return results.stream().map(MetadataResultEntry::getMetadataEntry).collect(Collectors.toList());
  }

  public List<String> getCursors() {
    return cursors;
  }
}
