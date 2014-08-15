/*
 * Copyright 2014 Cask, Inc.
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
