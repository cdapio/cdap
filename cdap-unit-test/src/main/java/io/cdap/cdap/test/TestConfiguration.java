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

package co.cask.cdap.test;

import com.google.common.base.Preconditions;
import org.junit.rules.ExternalResource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * This class can be used to setup CDAP configuration for unit-test.
 * <p>
 * Usage:
 *
 * <pre>{@code
 * class MyUnitTest extends TestBase {
 *
 *   &#64;ClassRule
 *   public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", "false");
 *
 *   ....
 * }
 * }</pre>
 *
 * </p>
 */
public class TestConfiguration extends ExternalResource {

  public static final String PROPERTY_PREFIX = "cdap.unit.test.";
  private final Map<String, String> configs;

  /**
   * Creates a new instance with the give list of configurations.
   *
   * @param configs list of configuration pairs.
   *                The list must be in the form of {@code (key1, value1, key2, value2, ...)},
   *                hence the length of configs must be even.
   *                The {@link Object#toString()} method will be called to obtain the keys and values that go into
   *                the configuration.
   */
  public TestConfiguration(Object... configs) {
    Preconditions.checkArgument(configs.length % 2 == 0,
                                "Arguments must be in pair form like (k1, v1, k2, v2): %s", Arrays.toString(configs));

    this.configs = new HashMap<>();
    for (int i = 0; i < configs.length; i += 2) {
      this.configs.put(PROPERTY_PREFIX + configs[i].toString(), configs[i + 1].toString());
    }
  }

  /**
   * Creates a new instance with the given configurations.
   *
   * @param configs a Map of configurations
   */
  public TestConfiguration(Map<String, String> configs) {
    this.configs = new HashMap<>(configs);
  }

  @Override
  protected void before() throws Throwable {
    // Use the system properties map as a mean to communicate unit-test specific CDAP configurations to the
    // TestBase class, which it will use to setup the CConfiguration.
    // All keys set into the properties are prefixed with PROPERTY_PREFIX.

    // It is done using @ClassRule instead of inheritance/method override is because TestBase starts
    // CDAP services in a @BeforeClass method, which is a static method.
    //
    // In JUnit, @BeforeClass methods in super-class are called before the one in the sub-class.
    // This means the sub-class cannot use @BeforeClass method to provide configurations before CDAP initialize.
    //
    // Since @BeforeClass method must be static, there is no way to guarantee that the sub-class implements
    // a particular method such that TestBase can calls to get configurations before initializing CDAP.
    // One can rely on naming convention and static method shadowing, however, that is an anti-pattern.
    // Using @ClassRule gives a much cleaner solution.
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      System.setProperty(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected void after() {
    for (String key : configs.keySet()) {
      System.clearProperty(key);
    }
  }
}
