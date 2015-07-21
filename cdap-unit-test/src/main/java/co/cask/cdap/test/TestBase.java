/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

import org.junit.BeforeClass;
import org.junit.ClassRule;

/**
 * Base class to inherit from for unit-test.
 * It provides testing functionality for {@link co.cask.cdap.api.app.Application}.
 * To clean App Fabric state, you can use the {@link #clear} method.
 * <p>
 * Custom configurations for CDAP can be set by using {@link ClassRule} and {@link TestConfiguration}.
 * </p>
 *
 * @see TestConfiguration
 */
public class TestBase extends ConfigurableTestBase {

  @BeforeClass
  public static void init() throws Exception {
    initTestBase(null);
  }
}
