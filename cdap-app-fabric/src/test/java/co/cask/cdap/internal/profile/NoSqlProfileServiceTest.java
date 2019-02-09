/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.internal.profile;

import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.spi.data.StructuredTableAdmin;
import com.google.common.base.Joiner;
import com.google.inject.Injector;
import org.junit.BeforeClass;

/**
 * Test Profile Service using NoSQL storage.
 */
public class NoSqlProfileServiceTest extends ProfileServiceTest {
  private static Injector injector;

  @BeforeClass
  public static void setup() {
    CConfiguration cConf = CConfiguration.create();
    // any plugin which requires transaction will be excluded
    cConf.set(Constants.REQUIREMENTS_DATASET_TYPE_EXCLUDE, Joiner.on(",").join(Table.TYPE, KeyValueTable.TYPE));
    cConf.set(Constants.Dataset.DATA_STORAGE_IMPLEMENTATION, Constants.Dataset.DATA_STORAGE_NOSQL);
    injector = AppFabricTestHelper.getInjector(cConf);
  }

  @Override
  protected Injector getInjector() {
    return injector;
  }

  @Override
  protected ProfileService getProfileService() {
    return injector.getInstance(ProfileService.class);
  }

  @Override
  protected StructuredTableAdmin getTableAdmin() {
    return injector.getInstance(StructuredTableAdmin.class);
  }
}
