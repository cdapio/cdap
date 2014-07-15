/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.internal.specification;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.internal.lang.FieldVisitor;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * A {@link FieldVisitor} that extracts {@link DataSetSpecification} from embedded DataSet.
 */
public final class EmbeddedDataSetExtractor extends FieldVisitor {

  private final Map<String, DataSetSpecification> dataSetSpecs;

  public EmbeddedDataSetExtractor(Map<String, DataSetSpecification> dataSetSpecs) {
    this.dataSetSpecs = dataSetSpecs;
  }

  @Override
  public void visit(Object instance, TypeToken<?> inspectType, TypeToken<?> declareType, Field field) throws Exception {
    if (DataSet.class.isAssignableFrom(field.getType())) {
      DataSet dataSet = (DataSet) field.get(instance);
      if (dataSet == null) {
        // Ignore the field if it is null.
        return;
      }

      DataSetSpecification specification = dataSet.configure();
      // Key to DataSetSpecification is "className.fieldName" to avoid name collision.
      String key = declareType.getRawType().getName() + '.' + field.getName();
      dataSetSpecs.put(key, specification);
    }
  }
}
