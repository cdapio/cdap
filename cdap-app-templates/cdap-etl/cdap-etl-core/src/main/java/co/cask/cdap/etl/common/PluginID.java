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

package co.cask.cdap.etl.common;

import com.google.common.base.Splitter;

import java.util.Iterator;
import java.util.Objects;

/**
 * Id of a plugin. For example, transform.script.4 means it is a script transform that is the 4th stage in the pipeline.
 * Stage number is actually enough to be an id but it is not human readable.
 */
public class PluginID {
  private final String type;
  private final String name;
  private final int stage;

  public static PluginID from(String idStr) {
    Iterator<String> parts = Splitter.on(':').split(idStr).iterator();
    return new PluginID(parts.next(), parts.next(), Integer.valueOf(parts.next()));
  }

  public static PluginID from(String type, String name, int stage) {
    return new PluginID(type, name, stage);
  }

  private PluginID(String type, String name, int stage) {
    this.type = type;
    this.name = name;
    this.stage = stage;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public int getStage() {
    return stage;
  }

  public String getMetricsContext() {
    return String.format("%s.%s.%d", type, name, stage);
  }

  public String getID() {
    return String.format("%s:%s:%d", type, name, stage);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PluginID that = (PluginID) o;

    return Objects.equals(type, that.type) &&
      Objects.equals(name, that.name) &&
      stage == that.stage;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, name, stage);
  }
}
