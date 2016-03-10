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

package co.cask.cdap.etl.realtime.mock;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.Transform;

import java.util.HashMap;

/**
 * Transform that doubles every record it receives.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Double")
public class DoubleTransform extends Transform<StructuredRecord, StructuredRecord> {

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(input);
    emitter.emit(input);
  }

  public static co.cask.cdap.etl.proto.v1.Plugin getPlugin() {
    return new co.cask.cdap.etl.proto.v1.Plugin("Double", new HashMap<String, String>());
  }
}
