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

package co.cask.cdap.etl.batch.mock;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.Transform;

import java.util.HashMap;

/**
 * Transform used to test writing to error datasets. Writes all its input as errors.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Error")
public class ErrorTransform extends Transform<StructuredRecord, StructuredRecord> {

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emitError(new InvalidEntry<>(500, "msg", input));
  }

  public static co.cask.cdap.etl.proto.v1.Plugin getPlugin() {
    return new co.cask.cdap.etl.proto.v1.Plugin("Error", new HashMap<String, String>());
  }
}
