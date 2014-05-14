package com.continuuity.data2.dataset2.lib.table.hbase;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.lib.AbstractDatasetDefinition;
import com.continuuity.data2.dataset2.lib.continuuity.CConfigurationAware;
import com.continuuity.data2.dataset2.lib.fs.LocationFactoryAware;
import com.continuuity.data2.dataset2.lib.hbase.HBaseConfigurationAware;
import com.continuuity.data2.dataset2.lib.table.ConflictDetection;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;

import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

import java.io.IOException;

/**
 *
 */
public class HBaseOrderedTableDefinition
  extends AbstractDatasetDefinition<HBaseOrderedTable, HBaseOrderedTableAdmin>
  implements HBaseConfigurationAware, LocationFactoryAware, CConfigurationAware {

  private Configuration hConf;
  // todo: figure out right way to inject
  @Inject
  private HBaseTableUtil hBaseTableUtil;
  private LocationFactory locationFactory;
  // todo: datasets should not depend on continuuity configuration!
  private CConfiguration conf;

  public HBaseOrderedTableDefinition(String name) {
    super(name);
  }

  @Override
  public void setHBaseConfig(Configuration hConf) {
    this.hConf = hConf;
  }

  @Override
  public void setLocationFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Override
  public void setCConfiguration(CConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public DatasetInstanceSpec configure(String name, DatasetInstanceProperties properties) {
    return new DatasetInstanceSpec.Builder(name, getName())
      .properties(properties.getProperties())
      .build();
  }

  @Override
  public HBaseOrderedTable getDataset(DatasetInstanceSpec spec) throws IOException {
    ConflictDetection conflictDetection =
      ConflictDetection.valueOf(spec.getProperty("conflict.level", ConflictDetection.ROW.name()));
    // -1 means no purging, keep data "forever"
    Integer ttl = Integer.valueOf(spec.getProperty("ttl", "-1"));
    return new HBaseOrderedTable(spec.getName(), hConf, conflictDetection, ttl);
  }

  @Override
  public HBaseOrderedTableAdmin getAdmin(DatasetInstanceSpec spec) throws IOException {
    return new HBaseOrderedTableAdmin(spec, hConf, hBaseTableUtil, conf, locationFactory);
  }
}
