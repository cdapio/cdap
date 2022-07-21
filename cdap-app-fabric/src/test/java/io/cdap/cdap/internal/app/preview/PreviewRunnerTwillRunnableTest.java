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

package io.cdap.cdap.internal.app.preview;

import com.google.inject.Injector;
import io.cdap.cdap.app.preview.DefaultPreviewRunnerManager;
import io.cdap.cdap.app.preview.PreviewRunner;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Unit test for {@link PreviewRunnerTwillRunnable}.
 */
public class PreviewRunnerTwillRunnableTest {

  @Test
  public void testNoSQLInjector() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    Injector injector = PreviewRunnerTwillRunnable.createInjector(cConf, new Configuration(),
                                                                  new PreviewRequestPollerInfo(0, "testuid"));
    DefaultPreviewRunnerManager defaultPreviewRunnerManager = (DefaultPreviewRunnerManager) injector
      .getInstance(PreviewRunnerManager.class);
    Injector previewInjector = defaultPreviewRunnerManager.createPreviewInjector();
    previewInjector.getInstance(PreviewRunner.class);
  }

  @Test
  public void testPostgresQLInjector() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_SQL);
    Injector injector = PreviewRunnerTwillRunnable.createInjector(cConf, new Configuration(),
                                                                  new PreviewRequestPollerInfo(0, "testuid"));
    DefaultPreviewRunnerManager defaultPreviewRunnerManager = (DefaultPreviewRunnerManager) injector
      .getInstance(PreviewRunnerManager.class);
    Injector previewInjector = defaultPreviewRunnerManager.createPreviewInjector();
    previewInjector.getInstance(PreviewRunner.class);
  }
}
