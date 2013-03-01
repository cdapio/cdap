package com.continuuity.internal.io;

import com.continuuity.internal.api.Preconditions;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 *
 */
public class ReflectionFieldAccessorFactory implements FieldAccessorFactory {

  private final LoadingCache<FieldEntry, FieldAccessor> fieldAccessorCache;

  public ReflectionFieldAccessorFactory() {
    this.fieldAccessorCache = CacheBuilder.newBuilder().build(new CacheLoader<FieldEntry, FieldAccessor>() {
      @Override
      public FieldAccessor load(FieldEntry fieldEntry) throws Exception {
        Field field = null;
        for(TypeToken<?> type : fieldEntry.getType().getTypes().classes()) {
          try {
            field = type.getRawType().getDeclaredField(fieldEntry.getFieldName());
          } catch(NoSuchFieldException e) {
            // It's ok, keep searching.
            continue;
          }
          break;
        }

        Preconditions.checkNotNull(field, "No such field in type. Type: %s, field: %s",
                                   fieldEntry.getType(), fieldEntry.getFieldName());
        if (!field.isAccessible()) {
          field.setAccessible(true);
        }
        final Field finalField = field;
        final TypeToken<?> fieldType = fieldEntry.getType().resolveType(finalField.getGenericType());
        return new FieldAccessor() {
          @Override
          public void set(Object object, Object value) {
            try {
              finalField.set(object, value);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public Object get(Object object) {
            try {
              return finalField.get(object);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public TypeToken<?> getType() {
            return fieldType;
          }
        };
      }
    });
  }


  @Override
  public FieldAccessor getFieldAccessor(TypeToken<?> type, String fieldName) {
    return fieldAccessorCache.getUnchecked(new FieldEntry(type, fieldName));
  }

  private static final class FieldEntry {
    private final TypeToken<?> type;
    private final String fieldName;

    private FieldEntry(TypeToken<?> type, String fieldName) {
      this.type = type;
      this.fieldName = fieldName;
    }

    public TypeToken<?> getType() {
      return type;
    }

    public String getFieldName() {
      return fieldName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FieldEntry other = (FieldEntry) o;
      return type.equals(other.type) && fieldName.equals(other.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(type, fieldName);
    }
  }
}
