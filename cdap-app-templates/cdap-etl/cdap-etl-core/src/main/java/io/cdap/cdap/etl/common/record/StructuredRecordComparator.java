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

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.common.Schemas;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Gets a comparator for a value stored in a {@link StructuredRecord}.
 */
public class StructuredRecordComparator implements Comparator<StructuredRecord> {
  private static final Comparator<Schema> SCHEMA_COMPARATOR = new SchemaComparator();
  private static final Comparator<Object> NULL_COMPARATOR = (x, y) -> 0;
  private static final Comparator<Object> INT_COMPARATOR = Comparator.comparingInt(x -> (int) x);
  private static final Comparator<Object> LONG_COMPARATOR = Comparator.comparingLong(x -> (long) x);
  private static final Comparator<Object> FLOAT_COMPARATOR = Comparator.comparing(x -> (float) x);
  private static final Comparator<Object> DOUBLE_COMPARATOR = Comparator.comparingDouble(x -> (double) x);
  private static final Comparator<Object> STRING_COMPARATOR = Comparator.comparing(x -> (String) x);
  private static final Comparator<Object> BOOL_COMPARATOR = Comparator.comparing(x -> (Boolean) x);
  private static final Comparator<Object> ENUM_COMPARATOR = Comparator.comparingInt(x -> ((Enum) x).ordinal());

  @Override
  public int compare(StructuredRecord r1, StructuredRecord r2) {
    return compareRecords(r1, r2);
  }

  private Comparator<Object> getComparator(String fieldName, Schema schema) {
    switch (schema.getType()) {
      case NULL:
        return NULL_COMPARATOR;
      case INT:
        return INT_COMPARATOR;
      case LONG:
        return LONG_COMPARATOR;
      case FLOAT:
        return FLOAT_COMPARATOR;
      case DOUBLE:
        return DOUBLE_COMPARATOR;
      case STRING:
        return STRING_COMPARATOR;
      case BOOLEAN:
        return BOOL_COMPARATOR;
      case ENUM:
        return ENUM_COMPARATOR;
      case BYTES:
        return (x, y) -> Bytes.compareTo(asBytes(fieldName, x), asBytes(fieldName, y));
      case RECORD:
        return (x, y) -> compareRecords((StructuredRecord) x, (StructuredRecord) y);
      case ARRAY:
        return (x, y) -> compareArrays(fieldName, schema.getComponentSchema(), x, y);
      case MAP:
        //noinspection unchecked
        return (x, y) -> compareMaps(fieldName, schema.getMapSchema(),
                                     (Map<Object, Object>) x, (Map<Object, Object>) y);
      case UNION:
        //noinspection ConstantConditions
        return (x, y) -> compareUnions(fieldName, schema.getUnionSchemas(), x, y);
    }

    // should never happen
    throw new IllegalStateException(String.format("Cannot compare field '%s' of unexpected type '%s'",
                                                  fieldName, schema.getType()));
  }

  private int compareRecords(StructuredRecord r1, StructuredRecord r2) {
    int comp = SCHEMA_COMPARATOR.compare(r1.getSchema(), r2.getSchema());
    if (comp != 0) {
      return comp;
    }

    // both records must have the same fields, otherwise their schemas would be different
    //noinspection ConstantConditions
    for (Schema.Field field : r1.getSchema().getFields()) {
      Comparator<Object> comparator = getComparator(field.getName(), field.getSchema());
      comp = comparator.compare(r1.get(field.getName()), r2.get(field.getName()));
      if (comp != 0) {
        return comp;
      }
    }

    return 0;
  }

  private int compareUnions(String fieldName, List<Schema> schemas, Object val1, Object val2) {
    Supplier<IllegalArgumentException> schemaNotFoundException = () -> new IllegalArgumentException(
      String.format("A value for field '%s' is not any of the expected types in its union schema.", fieldName));
    Schema val1Schema = schemas.stream().filter(s -> matchesSchema(val1, s))
      .findFirst().orElseThrow(schemaNotFoundException);
    Schema val2Schema = schemas.stream().filter(s -> matchesSchema(val2, s))
      .findFirst().orElseThrow(schemaNotFoundException);

    int comp = SCHEMA_COMPARATOR.compare(val1Schema, val2Schema);
    if (comp != 0) {
      return comp;
    }

    return getComparator(fieldName, val1Schema).compare(val1, val2);
  }

  /**
   * Returns whether the given val is of the specified schema.
   * This method assumes the provided schema is one of the union schemas for the provided value.
   * Unions cannot contain multiple arrays or multiple maps, so there is no need to check the types
   * within a Map or Collection.
   */
  private boolean matchesSchema(Object val, Schema schema) {
    switch (schema.getType()) {
      case NULL:
        return val == null;
      case INT:
        return val instanceof Integer;
      case LONG:
        return val instanceof Long;
      case FLOAT:
        return val instanceof Float;
      case DOUBLE:
        return val instanceof Double;
      case STRING:
        return val instanceof String;
      case BOOLEAN:
        return val instanceof Boolean;
      case ENUM:
        return val instanceof Enum;
      case BYTES:
        return val instanceof ByteBuffer || val instanceof byte[];
      case RECORD:
        if (val instanceof StructuredRecord) {
          StructuredRecord s = (StructuredRecord) val;
          return Schemas.equalsIgnoringRecordName(s.getSchema(), schema);
        } else {
          return false;
        }
      case ARRAY:
        return val instanceof Collection || val.getClass().isArray();
      case MAP:
        return val instanceof Map;
      case UNION:
        //noinspection ConstantConditions
        for (Schema s : schema.getUnionSchemas()) {
          if (matchesSchema(val, s)) {
            return true;
          }
        }
        return false;
    }
    return false;
  }

  private int compareMaps(String fieldName, Map.Entry<Schema, Schema> mapSchema,
                          Map<Object, Object> m1, Map<Object, Object> m2) {
    int comp = Integer.compare(m1.size(), m2.size());
    if (comp != 0) {
      return comp;
    }

    Iterator<Map.Entry<Object, Object>> m1Iter;
    Iterator<Map.Entry<Object, Object>> m2Iter;
    Comparator<Object> keyComparator = getComparator(fieldName, mapSchema.getKey());
    if (m1 instanceof SortedMap && m2 instanceof SortedMap) {
      m1Iter = m1.entrySet().iterator();
      m2Iter = m2.entrySet().iterator();
    } else {
      TreeMap<Object, Object> m1Copy = new TreeMap<>(keyComparator);
      m1Copy.putAll(m1);
      TreeMap<Object, Object> m2Copy = new TreeMap<>(keyComparator);
      m2Copy.putAll(m2);
      m1Iter = m1Copy.entrySet().iterator();
      m2Iter = m2Copy.entrySet().iterator();
    }

    Comparator<Object> valComparator = getComparator(fieldName, mapSchema.getValue());
    while (m1Iter.hasNext()) {
      Map.Entry<Object, Object> m1Entry = m1Iter.next();
      Map.Entry<Object, Object> m2Entry = m2Iter.next();

      comp = keyComparator.compare(m1Entry.getKey(), m2Entry.getKey());
      if (comp != 0) {
        return comp;
      }
      comp = valComparator.compare(m1Entry.getValue(), m2Entry.getValue());
      if (comp != 0) {
        return comp;
      }
    }
    return 0;
  }

  private int compareArrays(String fieldName, Schema componentSchema, Object val1, Object val2) {
    ArrayWrapper val1Array = new ArrayWrapper(fieldName, val1);
    ArrayWrapper val2Array = new ArrayWrapper(fieldName, val2);
    int comp = Integer.compare(val1Array.size, val2Array.size);
    if (comp != 0) {
      return comp;
    }

    Iterator<Object> val1Iter = val1Array.iterator();
    Iterator<Object> val2Iter = val2Array.iterator();

    Comparator<Object> comparator = getComparator(fieldName, componentSchema);
    while (val1Iter.hasNext()) {
      comp = comparator.compare(val1Iter.next(), val2Iter.next());
      if (comp != 0) {
        return comp;
      }
    }

    return 0;
  }

  private byte[] asBytes(String fieldName, Object val) {
    if (val instanceof byte[]) {
      return (byte[]) val;
    } else if (val instanceof ByteBuffer) {
      return Bytes.toBytes((ByteBuffer) val);
    }
    throw new IllegalArgumentException(String.format("Field '%s' is of type bytes but is of unexpected Java type '%s'",
                                                     fieldName, val.getClass().getName()));
  }

  /**
   * A wrapper around an "array" value that allows iterating through elements in the array and keeps track of its
   * total size. This is used because an "array" can be a Java array or a Java collection.
   */
  private static class ArrayWrapper implements Iterable<Object> {
    private final Object array;
    private final boolean isCollection;
    private final int size;

    private ArrayWrapper(String fieldName, Object array) {
      if (!(array instanceof Collection) && !array.getClass().isArray()) {
        throw new IllegalArgumentException(String.format(
          "Field '%s' is of type array but is a Java '%s' instead of an array or collection.",
          fieldName, array.getClass().getName()));
      }
      this.array = array;
      this.isCollection = array instanceof Collection;
      this.size = this.isCollection ? ((Collection) array).size() : Array.getLength(array);
    }

    @Override
    public Iterator<Object> iterator() {
      if (isCollection) {
        return ((Collection<Object>) array).iterator();
      }
      return new Iterator<Object>() {
        private int curr = 0;

        @Override
        public boolean hasNext() {
          return curr < size;
        }

        @Override
        public Object next() {
          if (!hasNext()) {
            throw new NoSuchElementException("There are no more elements in the iterator.");
          }
          Object val = Array.get(array, curr);
          curr++;
          return val;
        }
      };
    }
  }
}
