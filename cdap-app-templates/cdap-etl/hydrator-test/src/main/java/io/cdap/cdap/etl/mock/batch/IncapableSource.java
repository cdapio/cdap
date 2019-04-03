/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.etl.mock.batch;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.annotation.Requirements;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.cdap.etl.proto.v2.ETLPlugin;

import java.util.Collections;

/**
 * Source which has a requirement
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(IncapableSource.NAME)
@Requirements(datasetTypes = {Table.TYPE})
public class IncapableSource extends BatchSource<byte[], Row, StructuredRecord> {

  public static final String NAME = "IncapableSource";

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {

  }

  /**
   * @return {@link IncapableSource} as the ETLPlugin
   */
  public static ETLPlugin getPlugin() {
    return new ETLPlugin(IncapableSource.NAME, BatchSource.PLUGIN_TYPE, Collections.emptyMap(), null);
  }
}


