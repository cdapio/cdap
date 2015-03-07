/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.tools;

import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Upgrader to upgrade the log meta data
 */
public class LogUpgrader extends AbstractUpgrader {

  private FileMetaDataManager fileMetaDataManager;
  private DatasetFramework dsFramework;

  @Inject
  private LogUpgrader(LocationFactory locationFactory,
                      @Named("fileMetaDataManager") FileMetaDataManager fileMetaDataManager,
                      @Named("dsFramework") DatasetFramework dsFramework) {
    super(locationFactory);
    this.fileMetaDataManager = fileMetaDataManager;
    this.dsFramework = dsFramework;
  }

  @Override
  void upgrade() throws Exception {
    fileMetaDataManager.upgrade(dsFramework);
  }
}
