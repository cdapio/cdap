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

package co.cask.cdap.internal.io;

import java.lang.reflect.Type;

/**
 * A base implementation of {@link FieldAccessor} that throws {@link UnsupportedOperationException}
 * for all getter/setter methods, which are meant to be overridden by children class.
 */
public abstract class AbstractFieldAccessor implements FieldAccessor {

  private final Type type;

  protected AbstractFieldAccessor(Type type) {
    this.type = type;
  }

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
  public final Type getType() {
    return type;
  }
}
