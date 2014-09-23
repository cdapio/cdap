/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.common.cli;

import co.cask.cdap.common.cli.exception.MissingArgumentException;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Convenience class to read from a set of arguments.
 */
public class Arguments {

  private final Map<String, String> arguments;
  private final String rawInput;

  public Arguments(Map<String, String> arguments, String rawInput) {
    this.rawInput = rawInput;
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  public String getRawInput() {
    return rawInput;
  }

  public String get(String key) throws MissingArgumentException {
    String value = arguments.get(key);
    if (value == null) {
      throw new MissingArgumentException("Missing required argument: " + key);
    }

    return value;
  }

  public String get(String key, String defaultValue) {
    String value = arguments.get(key);
    return Objects.firstNonNull(value, defaultValue);
  }

  public Integer getInt(String key) throws MissingArgumentException {
    String value = arguments.get(key);
    if (value == null) {
      throw new MissingArgumentException("Missing required argument: " + key);
    }
    return Integer.parseInt(value);
  }

  public Integer getInt(String key, int defaultValue) {
    String value = arguments.get(key);
    if (value != null) {
      return Integer.parseInt(value);
    } else {
      return defaultValue;
    }
  }

  public Long getLong(String key) throws MissingArgumentException {
    String value = arguments.get(key);
    if (value == null) {
      throw new MissingArgumentException("Missing required argument: " + key);
    }
    return Long.parseLong(value);
  }

  public Long getLong(String key, long defaultValue) {
    String value = arguments.get(key);
    if (value != null) {
      return Long.parseLong(value);
    } else {
      return defaultValue;
    }
  }
}
