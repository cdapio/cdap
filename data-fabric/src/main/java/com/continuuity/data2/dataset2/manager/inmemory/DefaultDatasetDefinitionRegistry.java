package com.continuuity.data2.dataset2.manager.inmemory;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.leveldb.LevelDBOcTableService;
import com.continuuity.data2.dataset2.lib.continuuity.CConfigurationAware;
import com.continuuity.data2.dataset2.lib.fs.LocationFactoryAware;
import com.continuuity.data2.dataset2.lib.hbase.HBaseConfigurationAware;
import com.continuuity.data2.dataset2.lib.hbase.HBaseTableUtilAware;
import com.continuuity.data2.dataset2.lib.leveldb.LevelDBAware;
import com.continuuity.data2.util.hbase.HBaseTableUtil;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocationFactory;

/**
 * this is a hack for initializing system-level datasets for now
 */
public class DefaultDatasetDefinitionRegistry extends InMemoryDatasetDefinitionRegistry {
  // will be provided only in distributed mode for now
  @Inject(optional = true)
  @Named("HBaseOVCTableHandleHConfig")
  private final Configuration hConf = null;

  // will be provided only in distributed mode for now
  @Inject(optional = true)
  private final HBaseTableUtil hbaseTableUtil = null;

  // will be provided only in leveldb mode for now
  @Inject(optional = true)
  private final LevelDBOcTableService levelDBOcTableService = null;

  private final CConfiguration conf;
  private final LocationFactory locationFactory;

  @Inject
  public DefaultDatasetDefinitionRegistry(final CConfiguration conf,
                                          final LocationFactory locationFactory) {
    this.conf = conf;
    this.locationFactory = locationFactory;
  }

  @Override
  public void add(DatasetDefinition def) {
    if (def instanceof CConfigurationAware) {
      ((CConfigurationAware) def).setCConfiguration(conf);
    }
    if (def instanceof HBaseConfigurationAware) {
      ((HBaseConfigurationAware) def).setHBaseConfig(hConf);
    }
    if (def instanceof HBaseTableUtilAware) {
      ((HBaseTableUtilAware) def).setHBaseTableUtil(hbaseTableUtil);
    }
    if (def instanceof LocationFactoryAware) {
      ((LocationFactoryAware) def).setLocationFactory(locationFactory);
    }
    if (def instanceof LevelDBAware) {
      ((LevelDBAware) def).setLevelDBService(levelDBOcTableService);
    }
    super.add(def);
  }
}
