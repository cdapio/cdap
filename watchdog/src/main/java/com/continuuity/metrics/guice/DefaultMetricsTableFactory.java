/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.guice;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.EntityTable;
import com.continuuity.metrics.data.MetricsTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 *
 */
final class DefaultMetricsTableFactory implements MetricsTableFactory {

  private final CConfiguration cConf;
  private final OVCTableHandle tableHandle;
  private final EntityTable entityTable;

  @Inject
  DefaultMetricsTableFactory(CConfiguration cConf, OVCTableHandle tableHandle) {
    try {
      this.cConf = cConf;
      this.tableHandle = tableHandle;
      this.entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes(
                                          cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                                    MetricsConstants.DEFAULT_ENTITY_TABLE_NAME))));
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public MetricsTable create(int resolution) {
    try {
      String tableName = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                   MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + resolution;

      return new MetricsTable(entityTable, tableHandle.getTable(Bytes.toBytes(tableName)),
                              resolution, getRollTime(resolution));
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  private int getRollTime(int resolution) {
    String key = MetricsConstants.ConfigKeys.METRICS_TABLE_ROLL_TIME + "." + resolution;
    String value = cConf.get(key);
    if (value != null) {
      return cConf.getInt(key, MetricsConstants.DEFAULT_METRICS_TABLE_ROLL_TIME);
    }
    return cConf.getInt(MetricsConstants.ConfigKeys.METRICS_TABLE_ROLL_TIME,
                        MetricsConstants.DEFAULT_METRICS_TABLE_ROLL_TIME);
  }
}
