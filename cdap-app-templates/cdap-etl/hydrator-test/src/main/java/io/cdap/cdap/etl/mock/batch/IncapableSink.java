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

package io.cdap.cdap.etl.mock.batch;

import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.annotation.Requirements;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Put;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;

import java.util.Collections;


/**
 * Sink which has an requirement which can be meet with another special requirement
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(IncapableSink.NAME)
@Requirements(datasetTypes = {Table.TYPE, KeyValueTable.TYPE})
public class IncapableSink extends BatchSink<StructuredRecord, byte[], Put> {

  public static final String NAME = "IncapableSink";

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {

  }

  /**
   * @return {@link IncapableSink} as the ETLPlugin
   */
  public static ETLPlugin getPlugin() {
    return new ETLPlugin(IncapableSink.NAME, BatchSink.PLUGIN_TYPE, Collections.emptyMap(), null);
  }
}


