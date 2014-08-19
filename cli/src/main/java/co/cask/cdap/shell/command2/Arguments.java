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

package co.cask.cdap.shell.command2;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.MissingArgumentException;

import java.util.Map;

/**
 *
 */
public class Arguments {

  private final Map<String, String> arguments;

  public Arguments(Map<String, String> arguments) {
    this.arguments = ImmutableMap.copyOf(arguments);
  }

  public String get(String key) throws MissingArgumentException {
    String value = arguments.get(key);
    if (value == null) {
      throw new MissingArgumentException("Missing required argument <" + key + ">");
    }

    return value;
  }

  public String get(String key, String defaultValue) {
    String value = arguments.get(key);
    return Objects.firstNonNull(value, defaultValue);
  }
}
