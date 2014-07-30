/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.dataset2.module.lib.hbase;

import com.continuuity.api.dataset.module.DatasetDefinitionRegistry;
import com.continuuity.api.dataset.module.DatasetModule;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseMetricsTable;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseMetricsTableDefinition;

/**
 * Registers HBase-backed implementations of the metrics system datasets
 */
public class HBaseMetricsTableModule implements DatasetModule {
  @Override
  public void register(DatasetDefinitionRegistry registry) {
    registry.add(new HBaseMetricsTableDefinition(HBaseMetricsTable.class.getName()));
    registry.add(new HBaseMetricsTableDefinition(MetricsTable.class.getName()));
  }
}
