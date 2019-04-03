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
package io.cdap.cdap.internal.app.runtime;

import io.cdap.cdap.api.annotation.UseDataSet;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.internal.lang.FieldVisitor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

/**
 * A {@link FieldVisitor} that inject DataSet instance into fields marked with {@link UseDataSet}.
 */
public final class DataSetFieldSetter extends FieldVisitor {

  private final DatasetContext dataSetContext;

  public DataSetFieldSetter(DatasetContext dataSetContext) {
    this.dataSetContext = dataSetContext;
  }

  @Override
  public void visit(Object instance, Type inspectType, Type declareType, Field field) throws Exception {
    if (Dataset.class.isAssignableFrom(field.getType())) {
      UseDataSet useDataSet = field.getAnnotation(UseDataSet.class);
      if (useDataSet != null && !useDataSet.value().isEmpty()) {
        field.set(instance, dataSetContext.getDataset(useDataSet.value()));
      }
    }
  }
}
