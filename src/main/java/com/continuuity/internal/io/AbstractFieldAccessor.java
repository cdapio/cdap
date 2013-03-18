package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 *
 */
public abstract class AbstractFieldAccessor implements FieldAccessor {
  @Override
  public <T> void set(Object object, T value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public <T> T get(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public boolean getBoolean(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public byte getByte(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public char getChar(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public short getShort(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public int getInt(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public long getLong(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public float getFloat(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public double getDouble(Object object) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setBoolean(Object object, boolean value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setByte(Object object, byte value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setChar(Object object, char value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setShort(Object object, short value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setInt(Object object, int value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setLong(Object object, long value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setFloat(Object object, float value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public void setDouble(Object object, double value) {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public TypeToken<?> getType() {
    throw new UnsupportedOperationException("Method not supported.");
  }
}
