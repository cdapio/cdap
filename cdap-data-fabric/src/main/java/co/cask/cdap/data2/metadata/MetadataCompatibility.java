/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.data2.metadata;

import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.data2.metadata.dataset.SortInfo;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.SearchResponse;
import co.cask.cdap.spi.metadata.Sorting;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Utility methods to convert Metadata SPI classes to CDAP-5.x metadata format.
 */
public final class MetadataCompatibility {

  private MetadataCompatibility() { }

  /**
   * Convert a {@link SearchResponse} to 5.x {@link MetadataSearchResponse}.
   *
   * The 5.x convention was that the results only contain non-empty records.
   */
  public static MetadataSearchResponse toV5Response(SearchResponse response, @Nullable String scope) {
    Sorting sorting = response.getRequest().getSorting();
    return new MetadataSearchResponse(sorting != null ? sorting.toString() : SortInfo.DEFAULT.toString(),
                                      response.getOffset(), response.getLimit(),
                                      response.getCursor() == null ? 0 : 1,
                                      response.getTotalResults(),
                                      toV5Results(response.getResults()),
                                      response.getCursor() == null ? Collections.emptyList()
                                        : Collections.singletonList(response.getCursor()),
                                      response.getRequest().isShowHidden(),
                                      scope == null ? EnumSet.allOf(EntityScope.class)
                                        : EnumSet.of(EntityScope.valueOf(scope)));
  }

  /**
   * Convert a list of {@link MetadataRecord}s to an ordered set of 5.x {@link MetadataSearchResultRecord}s.
   *
   * The 5.x convention was that the results only contain non-empty records.
   */
  private static Set<MetadataSearchResultRecord> toV5Results(List<MetadataRecord> results) {
    Set<MetadataSearchResultRecord> records = new LinkedHashSet<>();
    for (MetadataRecord record : results) {
     Map<MetadataScope, co.cask.cdap.api.metadata.Metadata> map = toV5Metadata(record.getMetadata());
     records.add(new MetadataSearchResultRecord(record.getEntity(), Maps.filterValues(
       map, meta -> meta != null && !(meta.getProperties().isEmpty() && meta.getTags().isEmpty()))));
    }
    return records;
  }

  /**
   * Convert a {@link Metadata} to a 5.x map from scope to {@link co.cask.cdap.api.metadata.Metadata}.
   *
   * The 5.x convention was that the map contains all scopes even if their metadata is empty.
   */
  public static Map<MetadataScope, co.cask.cdap.api.metadata.Metadata> toV5Metadata(Metadata metadata) {
    return MetadataScope.ALL.stream().collect(Collectors.toMap(
      scope -> scope, scope -> toV5Metadata(metadata, scope)));
  }

  /**
   * Convert a {@link Metadata} to a 5.x {@link co.cask.cdap.api.metadata.Metadata} for a given scope.
   */
  public static co.cask.cdap.api.metadata.Metadata toV5Metadata(Metadata metadata, MetadataScope scope) {
    return new co.cask.cdap.api.metadata.Metadata(metadata.getProperties(scope), metadata.getTags(scope));
  }

  /**
   * Convert a {@link Metadata} to a 5.x map from scope to {@link co.cask.cdap.api.metadata.Metadata}.
   */
  public static Set<co.cask.cdap.common.metadata.MetadataRecord>
  toV5MetadataRecords(MetadataRecord record, @Nullable String requestedScope) {
    Set<co.cask.cdap.common.metadata.MetadataRecord> result = new HashSet<>();
    for (MetadataScope scope : MetadataScope.ALL) {
      Set<String> tags = record.getMetadata().getTags(scope);
      Map<String, String> properties = record.getMetadata().getProperties(scope);
      if (requestedScope == null || scope.name().equalsIgnoreCase(requestedScope)) {
        result.add(new co.cask.cdap.common.metadata.MetadataRecord(record.getEntity(), scope, properties, tags));
      }
    }
    return result;
  }
}
