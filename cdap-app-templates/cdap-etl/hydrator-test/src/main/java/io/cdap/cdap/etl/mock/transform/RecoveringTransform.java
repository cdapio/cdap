/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.mock.transform;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transform that throws exception for first few calls and then recovers.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(RecoveringTransform.NAME)
public class RecoveringTransform extends Transform<StructuredRecord, StructuredRecord> {

  public static final PluginClass PLUGIN_CLASS = getPluginClass();
  public static final String NAME = "RecoveringTransform";
  private static AtomicInteger exceptionCounter = new AtomicInteger(0);

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter)
      throws Exception {
    if (exceptionCounter.getAndIncrement() < 3) {
      throw new RuntimeException("Exception in transform");
    }
    emitter.emit(input);
  }

  public static ETLPlugin getPlugin() {
    return new ETLPlugin(NAME, Transform.PLUGIN_TYPE, Collections.emptyMap(), null);
  }

  private static PluginClass getPluginClass() {
    return PluginClass.builder()
        .setName(NAME)
        .setType(Transform.PLUGIN_TYPE)
        .setDescription("")
        .setClassName(RecoveringTransform.class.getName())
        .setProperties(Collections.emptyMap())
        .build();
  }

  /**
   * Reset the static counter to start throwing exceptions again
   */
  public static void reset() {
    exceptionCounter.set(0);
  }
}
