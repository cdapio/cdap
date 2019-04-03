/*
 * Copyright © 2014 Cask Data, Inc.
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
package io.cdap.cdap.internal.specification;

import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.internal.lang.FieldVisitor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.Set;

/**
 * A {@link FieldVisitor} that extracts names of all {@link UseDataSet} fields.
 */
public final class DataSetFieldExtractor extends FieldVisitor {

  private final Set<String> dataSets;

  /**
   * Constructs a {@link DataSetFieldExtractor} that store DataSet names extracted from fields.
   * @param dataSets set to store datasets
   */
  public DataSetFieldExtractor(Set<String> dataSets) {
    this.dataSets = dataSets;
  }

  @Override
  public void visit(Object instance, Type inspectType, Type declareType, Field field) {
    if (Dataset.class.isAssignableFrom(field.getType())) {
      UseDataSet dataset = field.getAnnotation(UseDataSet.class);
      if (dataset == null || dataset.value().isEmpty()) {
        return;
      }
      dataSets.add(dataset.value());
    }
  }
}
