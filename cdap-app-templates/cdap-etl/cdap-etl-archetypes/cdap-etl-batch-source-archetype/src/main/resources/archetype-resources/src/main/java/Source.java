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
package $package;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;

/**
 * Batch Source to poll data from external sources.
 */
@Plugin(type = "batchsource")
@Name("Source")
@Description("Batch Source")
public class Source extends BatchSource<byte[], byte[], StructuredRecord> {

  private final SourceConfig config;

  public Source(SourceConfig config) {
    this.config = config;
  }


  /**
   * Config class for Source.
   */
  public static class SourceConfig extends PluginConfig {

    @Name("param")
    @Description("Source Param")
    private String param;
    // Note:  only primitives (included boxed types) and string are the types that are supported

  }

  @Override
  public void initialize(BatchSourceContext context) throws Exception {
    super.initialize(context);
    // Get Config param and use to initialize
    // String param = config.param
    // Perform init operations, external operations etc.
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    // Configure hadoop job before running in batch.
  }

  @Override
  public void destroy() {
    // No-op
    // Handle destroy life
  }



  @Override
  public void transform(KeyValue<byte[], byte[]> input, Emitter<StructuredRecord> emitter) throws Exception {
    // emitter.emit(value);
  }
}


