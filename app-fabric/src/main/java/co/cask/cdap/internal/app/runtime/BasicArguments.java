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

package co.cask.cdap.internal.app.runtime;

import co.cask.cdap.app.runtime.Arguments;
import com.google.common.collect.ImmutableMap;

import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public final class BasicArguments implements Arguments {

  private final Map<String, String> options;

  public BasicArguments() {
    this(ImmutableMap.<String, String>of());
  }

  public BasicArguments(Map<String, String> options) {
    this.options = ImmutableMap.copyOf(options);
  }

  @Override
  public boolean hasOption(String optionName) {
    return options.containsKey(optionName);
  }

  @Override
  public String getOption(String name) {
    return options.get(name);
  }

  @Override
  public String getOption(String name, String defaultOption) {
    String value = getOption(name);
    return value == null ? defaultOption : value;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return options.entrySet().iterator();
  }
}
