package com.continuuity.internal.io;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Field;

/**
 *
 */
public final class ReflectionFieldAccessorFactory implements FieldAccessorFactory {

  private final LoadingCache<FieldEntry, FieldAccessor> fieldAccessorCache;

  public ReflectionFieldAccessorFactory() {
    this.fieldAccessorCache = CacheBuilder.newBuilder().build(new CacheLoader<FieldEntry, FieldAccessor>() {
      @Override
      public FieldAccessor load(FieldEntry fieldEntry) throws Exception {
        Field field = null;
        for (TypeToken<?> type : fieldEntry.getType().getTypes().classes()) {
          try {
            field = type.getRawType().getDeclaredField(fieldEntry.getFieldName());
          } catch (NoSuchFieldException e) {
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
          public <T> void set(Object object, T value) {
            try {
              finalField.set(object, value);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @SuppressWarnings("unchecked")
          @Override
          public <T> T get(Object object) {
            try {
              return (T) finalField.get(object);
            } catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public boolean getBoolean(Object object) {
            return (Boolean) get(object);
          }

          @Override
          public byte getByte(Object object) {
            return (Byte) get(object);
          }

          @Override
          public char getChar(Object object) {
            return (Character) get(object);
          }

          @Override
          public short getShort(Object object) {
            return (Short) get(object);
          }

          @Override
          public int getInt(Object object) {
            return (Integer) get(object);
          }

          @Override
          public long getLong(Object object) {
            return (Long) get(object);
          }

          @Override
          public float getFloat(Object object) {
            return (Float) get(object);
          }

          @Override
          public double getDouble(Object object) {
            return (Double) get(object);
          }

          @Override
          public void setBoolean(Object object, boolean value) {
            set(object, value);
          }

          @Override
          public void setByte(Object object, byte value) {
            set(object, value);
          }

          @Override
          public void setChar(Object object, char value) {
            set(object, value);
          }

          @Override
          public void setShort(Object object, short value) {
            set(object, value);
          }

          @Override
          public void setInt(Object object, int value) {
            set(object, value);
          }

          @Override
          public void setLong(Object object, long value) {
            set(object, value);
          }

          @Override
          public void setFloat(Object object, float value) {
            set(object, value);
          }

          @Override
          public void setDouble(Object object, double value) {
            set(object, value);
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
}
