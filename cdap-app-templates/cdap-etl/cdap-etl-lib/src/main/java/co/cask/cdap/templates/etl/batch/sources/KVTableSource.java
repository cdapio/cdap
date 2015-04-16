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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;

/**
 * CDAP Key Value Table Dataset Batch Source.
 */
public class KVTableSource extends BatchReadableSource<byte[], byte[]> {
  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KVTableSource");
    configurer.setDescription("CDAP KeyValue Table Dataset Batch Source");
    configurer.addProperty(new Property(NAME, "Dataset Name", true));
  }

  @Override
  protected String getType(ETLStage stageConfig) {
    return KeyValueTable.class.getName();
  }
}
