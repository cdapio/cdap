/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.util;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.app.store.SourceControlMetadataFilter;
import io.cdap.cdap.proto.sourcecontrol.SortBy;
import io.cdap.cdap.spi.data.SortOrder;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for building ScanSourceControlMetadataRequest.
 */
public class SourceControlMetadataHelper {

  private static final String FILTER_SPLITTER = "AND";
  private static final Pattern KEY_VALUE_PATTERN = Pattern.compile("(\"?)(\\w+)=(\\w+)(\"?)");


  /**
   * Constructs a ScanSourceControlMetadataRequest for retrieving source control metadata status.
   * This method creates a ScanSourceControlMetadataRequest object with specified parameters for
   * fetching source control metadata status.
   *
   * @param namespace The namespace to filter the metadata.
   * @param pageToken The token for pagination.
   * @param pageSize  The size of each page for pagination.
   * @param orderBy   The sorting order for the metadata.
   * @param sortOn    The field to sort the metadata on.
   * @param filter    The filter criteria for the metadata.
   * @return A ScanSourceControlMetadataRequest configured with the provided parameters.
   */
  public static ScanSourceControlMetadataRequest getScmStatusScanRequest(String namespace,
      String pageToken,
      Integer pageSize, SortOrder orderBy, SortBy sortOn, String filter) {
    ScanSourceControlMetadataRequest.Builder builder = ScanSourceControlMetadataRequest.builder();
    builder.setNamespace(namespace);
    if (pageSize != null) {
      builder.setLimit(pageSize);
    }
    if (pageToken != null && !pageToken.isEmpty()) {
      builder.setScanAfter(pageToken);
    }
    if (orderBy != null) {
      builder.setSortOrder(orderBy);
    }
    if (sortOn != null) {
      builder.setSortOn(sortOn);
    }
    if (filter != null && !filter.isEmpty()) {
      builder.setFilter(getFilter(filter));
    }

    return builder.build();
  }

  private static SourceControlMetadataFilter getFilter(String filterStr)
      throws IllegalArgumentException {
    ImmutableMap<String, String> filterKeyValMap = parseKeyValStr(filterStr, FILTER_SPLITTER);
    String name = null;
    Boolean isSynced = null;

    for (Map.Entry<String, String> entry : filterKeyValMap.entrySet()) {
      String filterValue = entry.getValue();
      SourceControlMetadataFilterKey filterKey = SourceControlMetadataFilterKey.valueOf(
          entry.getKey());

      try {
        switch (filterKey) {
          case NAME_CONTAINS:
            name = filterValue;
            break;
          case IS_SYNCED:
            if (filterValue.equals("TRUE")) {
              isSynced = true;
            } else if (filterValue.equals("FALSE")) {
              isSynced = false;
            } else {
              throw new IllegalArgumentException("Invalid value for IS_SYNCED: " + filterValue);
            }
            break;
          default:
            throw new IllegalArgumentException("Unknown filter key: " + filterKey);
        }
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid " + filterKey.name() + ": " + filterValue, e);
      }
    }
    return new SourceControlMetadataFilter(name, isSynced);
  }

  private static ImmutableMap<String, String> parseKeyValStr(String input, String splitter) {
    ImmutableMap.Builder<String, String> keyValMap = ImmutableMap.<String, String>builder();
    String[] keyValPairs = input.split(splitter);

    for (String keyValPair : keyValPairs) {
      Matcher matcher = KEY_VALUE_PATTERN.matcher(keyValPair.trim());

      if (matcher.matches()) {
        keyValMap.put(matcher.group(2).trim().toUpperCase(), matcher.group(3).trim().toUpperCase());
      } else {
        throw new IllegalArgumentException("Invalid filter key=val pair: " + keyValPair);
      }
    }
    return keyValMap.build();
  }

  /**
   * Enum representing the filter keys for source control metadata.
   */
  public enum SourceControlMetadataFilterKey {
    NAME_CONTAINS,
    IS_SYNCED
  }
}
