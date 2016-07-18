/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.common.internal.remote;

import com.google.gson.JsonElement;

import javax.annotation.Nullable;

/**
 * Allows for simple serialization/deserialization of a method argument.
 */
public final class MethodArgument {
  private final String type;
  private final JsonElement value;

  public MethodArgument(String type, JsonElement value) {
    this.type = type;
    this.value = value;
  }

  public String getType() {
    return type;
  }

  @Nullable
  public JsonElement getValue() {
    return value;
  }
}
