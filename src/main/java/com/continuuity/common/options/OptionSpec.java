package com.continuuity.common.options;

import java.lang.reflect.Field;

/**
 * Licensed to Odiago, Inc. under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Odiago, Inc.
 * licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
public class OptionSpec {
  private Field field;
  private Option option;
  private Object object;

  public OptionSpec(Field field, Option option, Object object)  {
    this.field = field;
    this.option = option;
    this.object = object;
  }

  public String getName() {
    return option.name().isEmpty() ? field.getName() : option.name();
  }

  public Class<?> getType() {
    return field.getType();
  }

  public String getTypeName() {
    Class<?> type = getType();
    if(type == String.class) {
      return "String";
    }
    return type.toString();
  }

  public boolean isHidden() {
    return option.hidden();
  }

  public String getUsage() {
    return option.usage();
  }

  public String getEnvVar() {
    return option.envVar();
  }

  public String getDefaultValue() {
    String value = "";
    try {
      // If private field, set the accessible to true to access it.
      if(! field.isAccessible()) {
        field.setAccessible(true);
      }

      // Now depending on the type of the field convert the field
      // to string type.
      if(field.getType() == int.class) {
        value = Integer.toString(field.getInt(object));
      } else if(field.getType() == long.class) {
        value = Long.toString(field.getLong(object));
      } else if(field.getType() == short.class) {
        value = Short.toString(field.getShort(object));
      } else if(field.getType() == float.class) {
        value = Float.toString(field.getFloat(object));
      } else if(field.getType() == double.class) {
        value = Double.toString(field.getDouble(object));
      } else if(field.getType() == boolean.class) {
        value = Boolean.toString(field.getBoolean(object));
      } else if(field.getType() == String.class) {
        String s = (String) field.get(object);
        if(s != null) {
          value = "\"" + s + "\"";
        } else {
          value = "null";
        }
      }
    } catch (IllegalAccessException e) {
      throw new IllegalAccessError(e.getMessage());
    }
    return value;
  }

  public void setValue(String value) throws IllegalAccessException {
    if (!field.isAccessible()) {
      field.setAccessible(true);
    }
    if (field.getType() == boolean.class) {
      if (value.equals("true") || value.isEmpty()) {
        field.setBoolean(object, true);
      } else if (value.equals("false")) {
        field.setBoolean(object, false);
      } else {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == double.class) {
      try {
        field.setDouble(object, Double.parseDouble(value));
      } catch (NumberFormatException e) {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == float.class) {
      try {
        field.setFloat(object, Float.parseFloat(value));
      } catch (NumberFormatException e) {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == int.class) {
      try {
        field.setInt(object, Integer.parseInt(value));
      } catch (NumberFormatException e) {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == long.class) {
      try {
        field.setLong(object, Long.parseLong(value));
      } catch (NumberFormatException e) {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == short.class) {
      try {
        field.setShort(object, Short.parseShort(value));
      } catch (NumberFormatException e) {
        throw new IllegalOptionValueException(getName(), value);
      }
    } else if (field.getType() == String.class) {
      field.set(object, value);
    } else {
      throw new UnsupportedOptionTypeException(field.getName());
    }
  }
  
}
