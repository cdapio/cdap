/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.relational;

import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implementation of {@link Relation} which takes multiple relations and delegates operations to all of them.
 */
public class DelegatingMultiRelation implements Relation {

  List<Relation> delegates;

  private DelegatingMultiRelation(List<Relation> delegates) {
    this.delegates = delegates;
  }

  @Override
  public boolean isValid() {
    return delegates.stream().allMatch(Relation::isValid);
  }

  @Override
  public String getValidationError() {
    return delegates.stream().map(Relation::getValidationError).collect(Collectors.joining(";"));
  }

  @Override
  public Relation setColumn(String column, Expression value) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.setColumn(column, value))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation dropColumn(String column) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.dropColumn(column))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation select(Map<String, Expression> columns) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.select(columns))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation filter(Expression filter) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.filter(filter))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation groupBy(GroupByAggregationDefinition aggregationDefinition) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.groupBy(aggregationDefinition))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation window(WindowAggregationDefinition aggregationDefinition) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.window(aggregationDefinition))
                               .collect(Collectors.toList()));
  }

  @Override
  public Relation deduplicate(DeduplicateAggregationDefinition aggregationDefinition) {
    return new DelegatingMultiRelation(delegates.stream()
                               .map(d -> d.deduplicate(aggregationDefinition))
                               .collect(Collectors.toList()));
  }

  public List<Relation> getDelegates() {
    return delegates;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  static class Builder {
    List<Relation> delegates;

    private Builder() {
      this.delegates = new ArrayList<>();
    }

    public void addRelation(Relation relation) {
      this.delegates.add(relation);
    }

    public DelegatingMultiRelation build() {
      return new DelegatingMultiRelation( this.delegates);
    }
  }
}
