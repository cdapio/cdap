/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.data2.metadata.system.AbstractSystemMetadataWriter;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Represents sorting info for search results.
 */
public class SortInfo {

  private static final Pattern SPACE_SPLIT_PATTERN = Pattern.compile("\\s+");

  /**
   * Default sort order, when no custom sorting is desired. Sorts search results as a function of relative weights for
   * the specified search query.
   */
  public static final SortInfo DEFAULT = new SortInfo(null, SortOrder.WEIGHTED);

  /**
   * Represents sorting order.
   */
  public enum SortOrder {
    ASC,
    DESC,
    WEIGHTED
  }

  private final String sortBy;
  private final SortOrder sortOrder;

  public SortInfo(@Nullable String sortBy, SortOrder sortOrder) {
    this.sortBy = sortBy;
    this.sortOrder = sortOrder;
  }

  /**
   * Returns the sort by column, unless the column does not matter, when the sort order is {@link SortOrder#WEIGHTED}.
   */
  @Nullable
  public String getSortBy() {
    return sortBy;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }

  /**
   * Parses a {@link SortInfo} object from the specified string. The supported format is
   * <pre>[sortBy][whitespace][sortOrder]</pre>.
   *
   * @param sort the string to parse into a {@link SortInfo}. If {@code null}, {@link #DEFAULT} is returned
   * @return the parsed {@link SortInfo}
   * @throws BadRequestException if the string does not conform to the expected format
   */
  public static SortInfo of(@Nullable String sort) throws BadRequestException {
    if (Strings.isNullOrEmpty(sort)) {
      return SortInfo.DEFAULT;
    }
    Iterable<String> sortSplit = Splitter.on(SPACE_SPLIT_PATTERN).trimResults().omitEmptyStrings().split(sort);
    if (Iterables.size(sortSplit) != 2) {
      throw new BadRequestException("'sort' parameter should be a space separated string containing the field " +
                                      "('name' or 'create_time') and the sort order ('asc' or 'desc'). Found " + sort);
    }
    Iterator<String> iterator = sortSplit.iterator();
    String sortBy = iterator.next();
    String sortOrder = iterator.next();
    if (!AbstractSystemMetadataWriter.ENTITY_NAME_KEY.equalsIgnoreCase(sortBy) &&
      !AbstractSystemMetadataWriter.CREATION_TIME_KEY.equalsIgnoreCase(sortBy)) {
      throw new BadRequestException("Sort field must be 'name' or 'create_time'. Found " + sortBy);
    }
    if (!"asc".equalsIgnoreCase(sortOrder) && !"desc".equalsIgnoreCase(sortOrder)) {
      throw new BadRequestException("Sort order must be one of 'asc' or 'desc'. Found " + sortOrder);
    }

    return new SortInfo(sortBy, SortInfo.SortOrder.valueOf(sortOrder.toUpperCase()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SortInfo that = (SortInfo) o;

    return Objects.equals(sortBy, that.sortBy) &&
      Objects.equals(sortOrder, that.sortOrder);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortBy, sortOrder);
  }
}
