/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.metrics.data;

import co.cask.cdap.data2.dataset2.DatasetManagementException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 *
 */
public class LocalTimeSeriesTableTest extends TimeSeriesTableTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();
  private static MetricsTableFactory tableFactory;

  @Override
  protected MetricsTableFactory getTableFactory() {
    return tableFactory;
  }

  @BeforeClass
  public static void init() throws IOException, DatasetManagementException {
    tableFactory = MetricsTestHelper.createLocalMetricsTableFactory(tmpFolder.newFolder());
  }
}
