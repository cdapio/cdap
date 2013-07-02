package com.continuuity.internal;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.DataSet;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;
import java.util.Set;

/**
 * Helper used for initializing program's specs.
 */
public final class ProgramSpecificationHelper {
  public static Set<String> inspectDataSets(Class<?> classToInspect, ImmutableSet.Builder<String> datasets) {
    TypeToken<?> type = TypeToken.of(classToInspect);

    // Walk up the hierarchy of the class.
    for (TypeToken<?> typeToken : type.getTypes().classes()) {
      if (typeToken.getRawType().equals(Object.class)) {
        break;
      }

      // Grab all the DataSet
      for (Field field : typeToken.getRawType().getDeclaredFields()) {
        if (DataSet.class.isAssignableFrom(field.getType())) {
          UseDataSet dataset = field.getAnnotation(UseDataSet.class);
          if (dataset == null || dataset.value().isEmpty()) {
            continue;
          }
          datasets.add(dataset.value());
        }
      }
    }

    return datasets.build();
  }

}
