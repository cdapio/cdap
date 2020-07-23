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

  private final CConfiguration previewCConf;
  private final Configuration previewHConf;
  private final SConfiguration previewSConf;
  private final LevelDBTableService previewLevelDBTableService;

  public PreviewConfigModule(CConfiguration cConf, Configuration hConf, SConfiguration sConf) {
    previewCConf = CConfiguration.copy(cConf);

    // Change all services bind address to local host
    String localhost = InetAddress.getLoopbackAddress().getHostName();
    StreamSupport.stream(previewCConf.spliterator(), false)
      .map(Map.Entry::getKey)
      .filter(s -> s.endsWith(".bind.address"))
      .forEach(key -> previewCConf.set(key, localhost));

    Path previewDataDir = Paths.get(cConf.get(Constants.CFG_LOCAL_DATA_DIR), "preview").toAbsolutePath();
    Path previewDir = null;
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

    // Setup Hadoop configuration
    previewHConf = new Configuration(hConf);
    previewHConf.set(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    previewHConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
                     previewDir.resolve("fs").toUri().toString());

    previewSConf = SConfiguration.copy(sConf);

    this.previewLevelDBTableService = new LevelDBTableService();
    this.previewLevelDBTableService.setConfiguration(previewCConf);
  }

  @Override
  protected void configure() {
    bind(CConfiguration.class).annotatedWith(Names.named(PREVIEW_CCONF)).toInstance(previewCConf);
    bind(Configuration.class).annotatedWith(Names.named(PREVIEW_HCONF)).toInstance(previewHConf);
    bind(SConfiguration.class).annotatedWith(Names.named(PREVIEW_SCONF)).toInstance(previewSConf);

    bind(LevelDBTableService.class)
      .annotatedWith(Names.named(PREVIEW_LEVEL_DB)).toInstance(previewLevelDBTableService);
  }
}
