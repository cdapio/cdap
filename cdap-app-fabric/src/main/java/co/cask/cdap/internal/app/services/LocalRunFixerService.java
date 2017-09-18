/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.services;

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import com.google.inject.Inject;

/**
 * A local-mode only run record corrector that corrects run records once upon app fabric server startup.
 */
public class LocalRunFixerService extends AbstractRunFixerService {

  @Inject
  public LocalRunFixerService(CConfiguration cConf, Store store, ProgramStateWriter programStateWriter,
                              ProgramLifecycleService programLifecycleService,
                              ProgramRuntimeService runtimeService, NamespaceAdmin namespaceAdmin,
                              DatasetFramework datasetFramework) {
    super(cConf, store, programStateWriter, programLifecycleService, runtimeService, namespaceAdmin, datasetFramework);
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    // In local mode, correct running records just once, on startup
    validateAndCorrectRunningRunRecords();
  }
}
