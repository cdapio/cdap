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

package co.cask.cdap.api.data.batch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Handy implementation of the {@link Split}. Acts as a map of attributes.
 */
public final class SimpleSplit extends Split {

  private final Map<String, String> attributes = new HashMap<>();

  /**
   * Sets an attribute.
   * @param name Name of the attribute.
   * @param value Value of the attribute.
   */
  public void set(String name, String value) {
    attributes.put(name, value);
  }

  /**
   * Gets an attribute value.
   * @param name Name of the attribute to get the value of.
   * @return value Value of the attribute, or null if not found.
   */
  public String get(String name) {
    return get(name, null);
  }

  /**
   * Gets an attribute value.
   * @param name Name of the attribute to get the value of.
   * @param defaultValue The value to return if the attribute is not found.
   * @return Value of the attribute, or the default value if the value is not found
   */
  public String get(String name, String defaultValue) {
    String value = attributes.get(name);
    return value == null ? defaultValue : value;
  }

  @Override
  public void writeExternal(DataOutput out) throws IOException {
    out.writeInt(attributes.size());
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      out.writeUTF(entry.getKey());
      out.writeUTF(entry.getValue());
    }
  }

  @Override
  public void readExternal(DataInput in) throws IOException {
    int len = in.readInt();
    for (int i = 0; i < len; i++) {
      attributes.put(in.readUTF(), in.readUTF());
    }
  }
}
