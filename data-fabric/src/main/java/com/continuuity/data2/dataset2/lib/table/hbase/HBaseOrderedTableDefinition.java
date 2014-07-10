package com.continuuity.data2.dataset2.lib.table.hbase;

import com.continuuity.api.dataset.DatasetProperties;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDatasetDefinition;
import com.continuuity.api.dataset.table.OrderedTable;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.ConflictDetection;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableClient;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseOrderedTableDefinition
  extends AbstractDatasetDefinition<OrderedTable, HBaseOrderedTableAdmin> {

  @Inject
  private Configuration hConf;
  @Inject
  private HBaseTableUtil hBaseTableUtil;
  @Inject
  private LocationFactory locationFactory;
  // todo: datasets should not depend on continuuity configuration!
  @Inject
  private CConfiguration conf;

  public HBaseOrderedTableDefinition(String name) {
    super(name);
  }

  @Override
  public DatasetSpecification configure(String name, DatasetProperties properties) {
    return DatasetSpecification.builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public OrderedTable getDataset(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    // -1 means no purging, keep data "forever"
    Integer ttl = Integer.valueOf(spec.getProperty("ttl", "-1"));
    return new HBaseOcTableClient(spec.getName(), conflictDetection, ttl, hConf);
  }

  @Override
  public HBaseOrderedTableAdmin getAdmin(DatasetSpecification spec, ClassLoader classLoader) throws IOException {
    return new HBaseOrderedTableAdmin(spec, hConf, hBaseTableUtil, conf, locationFactory);
  }
}
