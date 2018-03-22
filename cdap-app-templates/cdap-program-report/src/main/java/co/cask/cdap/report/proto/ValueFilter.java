/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report.proto;

import co.cask.cdap.report.util.ReportField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Represents a filter that checks whether a given value of a field is one of the allowed values and is not one of
 * the forbidden values.
 *
 * @param <T> type of the values
 */
public class ValueFilter<T> extends Filter<T> {
  @Nullable
  private final Set<T> whitelist;
  @Nullable
  private final Set<T> blacklist;

  public ValueFilter(String fieldName, @Nullable Set<T> whitelist, @Nullable Set<T> blacklist) {
    super(fieldName);
    this.whitelist = whitelist;
    this.blacklist = blacklist;
  }

  /**
   * @return the allowed values for this field, or an empty set if there's no such limit
   */
  public Set<T> getWhitelist() {
    return whitelist == null ? Collections.emptySet() : whitelist;
  }

  /**
   * @return the disallowed values for this field, or an empty set if there's no such limit
   */
  public Set<T> getBlacklist() {
    return blacklist == null ? Collections.emptySet() : blacklist;
  }

  @Override
  @Nullable
  public String getError() {
    List<String> errors = new ArrayList<>();
    String fieldError = super.getError();
    if (fieldError != null) {
      errors.add(fieldError);
    }
    errors.addAll(getFilterTypeErrors(ReportField.FilterType.VALUE));
    if (getWhitelist().isEmpty() && getBlacklist().isEmpty()) {
      errors.add("'whitelist' and 'blacklist' cannot both be empty");
    } else if (!getWhitelist().isEmpty() && !getBlacklist().isEmpty()) {
      Set<T> duplicates = new HashSet<>(getWhitelist());
      duplicates.retainAll(getBlacklist());
      if (!duplicates.isEmpty()) {
        errors.add("'whitelist' and 'blacklist' should not contain duplicated elements");
      }
    }
    return errors.isEmpty() ? null :
      String.format("Filter %s contains these errors: %s", getFieldName(), String.join("; ", errors));
  }

  @Override
  public boolean apply(T value) {
    return (getWhitelist().isEmpty() || getWhitelist().contains(value))
      && (getBlacklist().isEmpty() || !getBlacklist().contains(value));
  }

  @Override
  public String toString() {
    return "ValueFilter{" +
      "fieldName=" + getFieldName() +
      ", whitelist=" + whitelist +
      ", blacklist=" + blacklist +
      '}';
  }
}
