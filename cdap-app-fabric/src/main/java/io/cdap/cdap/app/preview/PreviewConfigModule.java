/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.app.preview;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.mapreduce.MRConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Module for creating preview configurations.
 */
public class PreviewConfigModule extends AbstractModule {
  public static final String PREVIEW_CCONF = "previewCConf";
  public static final String PREVIEW_HCONF = "previewHConf";
  public static final String PREVIEW_SCONF = "previewSConf";
  public static final String PREVIEW_LEVEL_DB = "previewLevelDB";
  public static final String GLOBAL_TMS = "globalTMS";
  public static final String GLOBAL_METRICS = "globalMetrics";


  private final CConfiguration previewCConf;
  private final Configuration previewHConf;
  private final SConfiguration previewSConf;

  public PreviewConfigModule(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    previewCConf = CConfiguration.copy(cConf);

    // Change all services bind address to local host
    String localhost = InetAddress.getLoopbackAddress().getHostName();
    StreamSupport.stream(previewCConf.spliterator(), false)
      .map(Map.Entry::getKey)
      .filter(s -> s.endsWith(".bind.address"))
      .forEach(key -> previewCConf.set(key, localhost));

    Path previewDataDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "preview").toAbsolutePath();
    Path previewDir;
    try {
      previewDir = Files.createDirectories(previewDataDir);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create preview dir: " + previewDataDir, e);
    }

    previewCConf.set(Constants.CFG_LOCAL_DATA_DIR, previewDir.toString());
    previewCConf.setIfUnset(Constants.CFG_DATA_LEVELDB_DIR, previewDir.toString());
    previewCConf.setBoolean(Constants.Explore.EXPLORE_ENABLED, false);
    // Use No-SQL store for preview data
    previewCConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);

    // Don't load custom log pipelines in preview
    previewCConf.unset(Constants.Logging.PIPELINE_CONFIG_DIR);

    previewCConf.set(Constants.Logging.TMS_TOPIC_PREFIX, "previewlog");
    previewCConf.setInt(Constants.Logging.NUM_PARTITIONS, 1);

    // Setup Hadoop configuration
    previewHConf = new Configuration(hConf);
    previewHConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    previewHConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                     previewDir.resolve("fs").toUri().toString());

    previewSConf = SConfiguration.copy(sConf);
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).annotatedWith(Names.named(PREVIEW_CCONF)).toInstance(previewCConf);
    bind(Configuration.class).annotatedWith(Names.named(PREVIEW_HCONF)).toInstance(previewHConf);
    bind(SConfiguration.class).annotatedWith(Names.named(PREVIEW_SCONF)).toInstance(previewSConf);
  }

  /**
   * Provider method to provide a singleton {@link LevelDBTableService}. A provider method is used instead of
   * instance binding with the {@link LinkedBindingBuilder#toInstance(Object)} method so that the
   * {@link LevelDBTableService#setConfiguration(CConfiguration)} is only called once from this method using
   * the preview cConf.
   */
  @Provides
  @Singleton
  @Named(PREVIEW_LEVEL_DB)
  private LevelDBTableService provideLevelDBTableService(@Named(PREVIEW_CCONF) CConfiguration cConf) {
    LevelDBTableService tableService = new LevelDBTableService();
    tableService.setConfiguration(cConf);
    return tableService;
  }
}
