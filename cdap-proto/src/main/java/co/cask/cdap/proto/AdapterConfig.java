/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.proto;

import java.util.Map;

/**
 * Specifies input parameters to create Adapter
 */
public final class AdapterConfig {
  public String type;
  public Map<String, String> properties;

  public Source source;
  public Sink sink;

  public String getType() {
    return type;
  }

  /**
   * Specifies the source of the Adapter
   */
  public static final class Source {
    public String name;
    public Map<String, String> properties;

    public Source() {
    }

    public Source(String name, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
    }
  }

  /**
   * Specifies the sink of the Adapter
   */
  public static final class Sink {
    public String name;
    public Map<String, String> properties;

    public Sink() {
    }

    public Sink(String name, Map<String, String> properties) {
      this.name = name;
      this.properties = properties;
    }
  }
}
