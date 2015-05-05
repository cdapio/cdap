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

package co.cask.cdap.test.base;

import co.cask.cdap.test.TestBase;
import org.junit.After;

import java.util.concurrent.TimeUnit;

/**
 * TestBase for all test framework tests
 */
public class TestFrameworkTestBase extends TestBase {

  @Override
  @After
  public void afterTest() throws Exception {
    try {
      super.afterTest();
      // Sleep a second before clear. There is a race between removal of RuntimeInfo
      // in the AbstractProgramRuntimeService class and the clear() method, which loops all RuntimeInfo.
      // The reason for the race is because removal is done through callback.
      TimeUnit.SECONDS.sleep(1);
    } finally {
      clear();
    }
  }
}
