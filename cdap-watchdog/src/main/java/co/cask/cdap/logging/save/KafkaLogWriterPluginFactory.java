/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.save;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.inject.Inject;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Factory to create {@link KafkaLogWriterPlugin}.
 */
public class KafkaLogWriterPluginFactory implements KafkaLogProcessorFactory {
  private final CConfiguration cConfig;
  private final FileMetaDataManager fileMetaDataManager;
  private final LocationFactory locationFactory;
  private final CheckpointManagerFactory checkpointManagerFactory;

  @Inject
  public KafkaLogWriterPluginFactory(CConfiguration cConfig, FileMetaDataManager fileMetaDataManager,
                                     LocationFactory locationFactory,
                                     CheckpointManagerFactory checkpointManagerFactory) {
    this.cConfig = cConfig;
    this.fileMetaDataManager = fileMetaDataManager;
    this.locationFactory = locationFactory;
    this.checkpointManagerFactory = checkpointManagerFactory;
  }

  @Override
  public KafkaLogProcessor create() throws Exception {
    return new KafkaLogWriterPlugin(cConfig, fileMetaDataManager, locationFactory, checkpointManagerFactory);
  }
}
