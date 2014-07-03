package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 *
 */
public interface FieldAccessor {

  <T> void set(Object object, T value);

  <T> T get(Object object);

  boolean getBoolean(Object object);

  byte getByte(Object object);

  char getChar(Object object);

  short getShort(Object object);

  int getInt(Object object);

  long getLong(Object object);

  float getFloat(Object object);

  double getDouble(Object object);

  void setBoolean(Object object, boolean value);

  void setByte(Object object, byte value);

  void setChar(Object object, char value);

  void setShort(Object object, short value);

  void setInt(Object object, int value);

  void setLong(Object object, long value);

  void setFloat(Object object, float value);

  void setDouble(Object object, double value);

  TypeToken<?> getType();
}
