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

package co.cask.cdap.proto.security;

import java.util.Collections;
import java.util.Map;

/**
 * Request for creating a new entry in the secure store
 */
public class SecureKeyCreateRequest {
  private final String description;
  private final String data;
  private final Map<String, String> properties;

  public SecureKeyCreateRequest(String description, String data, Map<String, String> properties) {
    this.description = description;
    this.data = data;
    this.properties = properties;
  }

  public String getDescription() {
    return description;
  }

  public String getData() {
    return data;
  }

  public Map<String, String> getProperties() {
    return properties == null ? Collections.<String, String>emptyMap() : properties;
  }

  @Override
  public String toString() {
    return "SecureKeyCreateRequest{" +
      "description='" + description + '\'' +
      ", data='" + data + '\'' +
      ", properties=" + properties +
      '}';
  }
}
