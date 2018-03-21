/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.report;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;

/**
 * An application that generates reports from program runs meta files.
 */
public class ReportGenerationApp extends AbstractApplication {
  public static final String NAME = "ReportGenerationApp";
  public static final String RUN_META_FILESET = "RunMetaFileset";
  public static final String REPORT_FILESET = "ReportFileset";

  @Override
  public void configure() {
    setName(NAME);
    addSpark(new ReportGenerationSpark());
    createDataset(REPORT_FILESET, FileSet.class, FileSetProperties.builder()
      .setEnableExploreOnCreate(false)
      .setDescription("fileSet")
      .build());
  }
}
