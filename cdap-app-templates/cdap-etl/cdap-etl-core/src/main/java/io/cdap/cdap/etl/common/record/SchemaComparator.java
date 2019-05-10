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

package io.cdap.cdap.etl.common.record;

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Compares schemas.
 *
 * If they are of different physical types, they are compared according to the natural ordering of their types.
 * If they are of different logical types, they are compared according to the natural ordering of their logical types.
 *
 * Union schemas are first compared by the number of schemas they have.
 * A union with fewer schemas is less than a union with more.
 * If the number of schemas are the same, the schemas are compared in order.
 *
 * Array schemas are compared by their component schemas.
 *
 * Record schemas are first compared by the number of fields they have.
 * A record with fewer fields is less than a record with more.
 * If the number of fields are the same, their fields are compared in order.
 *
 * A schema field is first compared by field name. If the names are the same, they are compared by schema.
 *
 * Map schemas are compared first by the key schema, then by the value schema.
 */
public class SchemaComparator implements Comparator<Schema> {

  @Override
  public int compare(Schema s1, Schema s2) {
    // check physical type
    int comp = s1.getType().compareTo(s2.getType());
    if (comp != 0) {
      return comp;
    }

    // check logical types
    if (s1.getLogicalType() == null && s2.getLogicalType() != null) {
      return -1;
    } else if (s1.getLogicalType() != null && s2.getLogicalType() == null) {
      return 1;
    } else if (s1.getLogicalType() != null) {
      comp = s1.getLogicalType().compareTo(s2.getLogicalType());
      if (comp != 0) {
        return comp;
      }
    }

    // must be the same physical and logical type
    switch (s1.getType()) {
      case UNION:
        //noinspection ConstantConditions
        return compareLists(s1.getUnionSchemas(), s2.getUnionSchemas(), this::compare);
      case ARRAY:
        return compare(s1.getComponentSchema(), s2.getComponentSchema());
      case MAP:
        //noinspection ConstantConditions
        return compareMapSchemas(s1.getMapSchema(), s2.getMapSchema());
      case RECORD:
        return compareRecordSchemas(s1, s2);
    }
    return 0;
  }

  private int compareMapSchemas(Map.Entry<Schema, Schema> s1, Map.Entry<Schema, Schema> s2) {
    int comp = compare(s1.getKey(), s2.getKey());
    if (comp != 0) {
      return comp;
    }
    return compare(s1.getValue(), s2.getValue());
  }

  private int compareRecordSchemas(Schema s1, Schema s2) {
    int nameComp = s1.getRecordName().compareTo(s2.getRecordName());
    if (nameComp != 0) {
      return nameComp;
    }

    //noinspection ConstantConditions
    return compareLists(s1.getFields(), s2.getFields(), (f1, f2) -> {
      int comp = f1.getName().compareTo(f2.getName());
      if (comp != 0) {
        return comp;
      }
      return compare(f1.getSchema(), f2.getSchema());
    });
  }

  private <T> int compareLists(List<T> l1, List<T> l2, BiFunction<T, T, Integer> comparison) {
    int comp = Integer.compare(l1.size(), l2.size());
    if (comp != 0) {
      return comp;
    }

    Iterator<T> iter1 = l1.iterator();
    Iterator<T> iter2 = l2.iterator();
    while (iter1.hasNext()) {
      comp = comparison.apply(iter1.next(), iter2.next());
      if (comp != 0) {
        return comp;
      }
    }
    return 0;
  }
}
