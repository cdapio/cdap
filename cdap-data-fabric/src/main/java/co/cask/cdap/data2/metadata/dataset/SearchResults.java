/*
 * Copyright 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata.dataset;

import java.util.List;

/**
 * Represents a list of {@link MetadataEntry} that match a search query in the {@link MetadataDataset}, along with a
 * list of cursors to start subsequent searches from.
 */
public class SearchResults {
  private final List<MetadataEntry> results;
  private final List<String> cursors;
  private final List<MetadataEntry> allResults;


  SearchResults(List<MetadataEntry> results, List<String> cursors, List<MetadataEntry> allResults) {
    this.results = results;
    this.cursors = cursors;
    this.allResults = allResults;
  }

  public List<MetadataEntry> getResults() {
    return results;
  }

  public List<String> getCursors() {
    return cursors;
  }

  public List<MetadataEntry> getAllResults() {
    return allResults;
  }
}
