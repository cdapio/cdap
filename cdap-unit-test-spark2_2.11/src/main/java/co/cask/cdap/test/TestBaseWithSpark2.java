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

package co.cask.cdap.test;

import co.cask.cdap.app.runtime.spark.SparkCompat;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.test.TestRunner;
import org.junit.ClassRule;
import org.junit.runner.RunWith;

/**
 * Base class to inherit from for unit-test that need to run Spark2.
 * It provides testing functionality for {@link co.cask.cdap.api.app.Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 * <p>
 * Custom configurations for CDAP can be set by using {@link ClassRule} and {@link TestConfiguration}.
 * </p>
 *
 * @see TestConfiguration
 */
@RunWith(TestRunner.class)
public class TestBaseWithSpark2 extends TestBase {

  // Do not overwrite (create a TestConfiguration with the exact same name),
  // otherwise Spark classes will not be available.
  @ClassRule
  public static final TestConfiguration DO_NOT_OVERWRITE_THIS_CONFIG =
    new TestConfiguration(Constants.AppFabric.SPARK_COMPAT, SparkCompat.SPARK2_2_11.getCompat());

}
