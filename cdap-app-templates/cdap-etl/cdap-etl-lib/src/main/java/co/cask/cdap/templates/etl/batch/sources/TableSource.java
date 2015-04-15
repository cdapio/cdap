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

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;

/**
 * CDAP Table Dataset Batch Source.
 */
public class TableSource extends BatchReadableSource<byte[], Row> {

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("TableSource");
    configurer.setDescription("CDAP Table Dataset Batch Source");
    configurer.addProperty(new Property(NAME, "Table Name", true));
    configurer.addProperty(new Property(
      DatasetProperties.SCHEMA,
      "Optional schema for the Table, which will be used when exploring the table. " +
        "Schema is only applied if the table does not already exist when the pipeline is created. ", false));
  }

  @Override
  protected String getType(ETLStage stageConfig) {
    return Table.class.getName();
  }
}
