/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.data;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.table.OVCTableHandle;
import com.continuuity.metrics.MetricsConstants;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 *
 */
public final class DefaultMetricsTableFactory implements MetricsTableFactory {

  private final CConfiguration cConf;
  private final OVCTableHandle tableHandle;
  private final MetricsEntityCodec entityCodec;

  @Inject
  public DefaultMetricsTableFactory(CConfiguration cConf, OVCTableHandle tableHandle) {
    try {
      this.cConf = cConf;
      this.tableHandle = tableHandle;
      EntityTable entityTable = new EntityTable(tableHandle.getTable(Bytes.toBytes(
                                                  cConf.get(MetricsConstants.ConfigKeys.ENTITY_TABLE_NAME,
                                                            MetricsConstants.DEFAULT_ENTITY_TABLE_NAME))));

      this.entityCodec = new MetricsEntityCodec(entityTable,
                                                MetricsConstants.DEFAULT_CONTEXT_DEPTH,
                                                MetricsConstants.DEFAULT_METRIC_DEPTH,
                                                MetricsConstants.DEFAULT_TAG_DEPTH);
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public TimeSeriesTable createTimeSeries(int resolution) {
    try {
      String tableName = cConf.get(MetricsConstants.ConfigKeys.METRICS_TABLE_PREFIX,
                                   MetricsConstants.DEFAULT_METRIC_TABLE_PREFIX) + ".ts." + resolution;

      return new TimeSeriesTable(tableHandle.getTable(Bytes.toBytes(tableName)), entityCodec,
                                 resolution, getRollTime(resolution));
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  private int getRollTime(int resolution) {
    String key = MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME + "." + resolution;
    String value = cConf.get(key);
    if (value != null) {
      return cConf.getInt(key, MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
    }
    return cConf.getInt(MetricsConstants.ConfigKeys.TIME_SERIES_TABLE_ROLL_TIME,
                        MetricsConstants.DEFAULT_TIME_SERIES_TABLE_ROLL_TIME);
  }
}
