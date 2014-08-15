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
package co.cask.cdap.common.options;

import java.lang.reflect.Field;

/**
 * Specifies an implementation for the specification of an Option.
 */
public class OptionSpec {
  /**
   * Instance of the field.
   */
  private Field field;

  /**
   * Specification of an object.
   */
  private Option option;

  /**
   * Object the field and specification are from.
   */
  private Object object;

  /**
   * Creates an instance of OptionSpec.
   * @param field annotated with @Option
   * @param option specifying information about the field that needs to be filled in by command line argument.
   * @param object specifies the object the option is specified in.
   */
  public OptionSpec(Field field, Option option, Object object)  {
    this.field = field;
    this.option = option;
    this.object = object;
  }

  /**
   * Returns the name of the field.
   * @return name of the field.
   */
  public String getName() {
    return option.name().isEmpty() ? field.getName() : option.name();
  }

  /**
   * Returns the type of the field.
   * @return type of the field.
   */
  public Class<?> getType() {
    return field.getType();
  }

  /**
   * Returns the type name of the field annotated by <code>@Option</code>.
   * @return name of the type of field.
   */
  public String getTypeName() {
    return option.type();
//    Class<?> type = getType();
//    if(type == String.class) {
//      return "String";
//    }
//    return type.toString();
  }

  /**
   * Returns whether the field needs to be hidden or no.
   * @return true if hidden; false otherwise.
   */
  public boolean isHidden() {
    return option.hidden();
  }

  /**
   * Returns the usage of the field.
   * @return String representation of usage.
   */
  public String getUsage() {
    return option.usage();
  }

  /**
   * Returns environment variable to be associated with the field annotated with @Option.
   * @return value of environment field.
   */
  public String getEnvVar() {
    return option.envVar();
  }

  /**
   * Returns string representation of default value.
   * @return default value of the field.
   */
  public String getDefaultValue() {
    String value = "";
    try {
      // If private field, set the accessible to true to access it.
      if (!field.isAccessible()) {
        field.setAccessible(true);
      }

      // Now depending on the type of the field convert the field
      // to string type.
      if (field.getType() == int.class) {
        value = Integer.toString(field.getInt(object));
      } else if (field.getType() == long.class) {
        value = Long.toString(field.getLong(object));
      } else if (field.getType() == short.class) {
        value = Short.toString(field.getShort(object));
      } else if (field.getType() == float.class) {
        value = Float.toString(field.getFloat(object));
      } else if (field.getType() == double.class) {
        value = Double.toString(field.getDouble(object));
      } else if (field.getType() == boolean.class) {
        value = Boolean.toString(field.getBoolean(object));
      } else if (field.getType() == String.class) {
        String s = (String) field.get(object);
        if (s != null) {
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

  /**
   * Sets the field with the value.
   * @param value to be set
   * @throws IllegalAccessException
   */
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
