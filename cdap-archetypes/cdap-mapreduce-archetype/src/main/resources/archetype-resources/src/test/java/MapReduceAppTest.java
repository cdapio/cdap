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

package $package;

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Test for {@link MapReduceApp}.
 */
public class MapReduceAppTest extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @Test
  public void test() throws Exception {
    // Deploy the HelloWorld application
    ApplicationManager appManager = deployApplication(MapReduceApp.class);

    // Uncomment this to start the MapReduce
    //MapReduceManager mapreduceManager = appManager.getMapReduceManager("MyMapReduce").start();

    // ...
  }
}
