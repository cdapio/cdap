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

package com.continuuity.data2.dataset.lib.table.hbase;

import com.continuuity.api.dataset.DatasetAdmin;
import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.MetricsTable;
import com.continuuity.data2.dataset2.lib.table.hbase.HBaseOrderedTableAdmin;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 * HBase based implementation for {@link MetricsTable}.
 */
public class HBaseMetricsTableDefinition extends AbstractDatasetDefinition<MetricsTable, DatasetAdmin> {
  @Inject
  private Configuration hConf;
  @Inject
  private HBaseTableUtil hBaseTableUtil;
  @Inject
  private LocationFactory locationFactory;
  @Inject
  private CConfiguration conf;

  public HBaseMetricsTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public MetricsTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new HBaseMetricsTable(spec.getName(), hConf);
  }

  @Override
  public HBaseOrderedTableAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new HBaseOrderedTableAdmin(spec, hConf, hBaseTableUtil, conf, locationFactory);
  }
}
