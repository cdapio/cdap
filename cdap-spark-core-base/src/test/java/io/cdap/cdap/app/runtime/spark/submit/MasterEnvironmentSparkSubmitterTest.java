/*
 *  Copyright © 2023 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.cdap.app.runtime.spark.submit;

import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for MasterEnvironment Spark submission.
 */
public class MasterEnvironmentSparkSubmitterTest {

  @Test
  public void testExtraJavaOptionsAppended() {
    String envOption = "-Dextra.java.opt=0";
    SparkConfig sparkConfig = new SparkConfig("master", URI.create("local://0"),
        Collections.emptyMap(), null, envOption);

    Map<String, String> appConf = new HashMap<>();
    String driverOption = "-Dapp.opt=driver";
    String executorOption = "-Dapp.opt=executor";
    appConf.put("spark.driver.extraJavaOptions", "-Dapp.opt=driver");
    appConf.put("spark.executor.extraJavaOptions", "-Dapp.opt=executor");
    Map<String, String> expected = new HashMap<>();
    expected.put("spark.driver.extraJavaOptions", envOption + " " + driverOption);
    expected.put("spark.executor.extraJavaOptions", envOption + " " + executorOption);

    Map<String, String> merged =
        MasterEnvironmentSparkSubmitter.mergeSparkConfs(appConf, sparkConfig);
    Assert.assertEquals(expected, merged);
  }
}
