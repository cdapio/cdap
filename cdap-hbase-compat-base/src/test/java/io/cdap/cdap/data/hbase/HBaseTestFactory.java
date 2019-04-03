/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package co.cask.cdap.data.hbase;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data2.util.hbase.HBaseVersionSpecificFactory;
import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * Factory class to provide instances of the correct {@link HBaseTestBase} implementation, dependent on the version
 * of HBase that is being used.
 *
 * <p>
 * Usage:
 *
 * <pre>{@code
 * class MyUnitTest {
 *
 *
 *   &#64;ClassRule
 *   public static final HBaseTestBase TEST_HBASE = new HBaseTestFactory().get();
 *
 *   ....
 * }
 * }</pre>
 *
 * </p>
 */
public class HBaseTestFactory extends HBaseVersionSpecificFactory<HBaseTestBase> {
  static final String PROPERTY_PREFIX = "cdap.hbase.test.";

  public HBaseTestFactory(Object... configs) {
    super(CConfiguration.create());
    Preconditions.checkArgument(configs.length % 2 == 0,
                                "Arguments must be in pair form like (k1, v1, k2, v2): %s", Arrays.toString(configs));

    for (int i = 0; i < configs.length; i += 2) {
      // Use the system properties map as a mean to communicate specific configurations to HBaseTestBase#startHBase
      System.setProperty(PROPERTY_PREFIX + configs[i].toString(), configs[i + 1].toString());
    }
  }

  @Override
  protected String getHBase96Classname() {
    return "co.cask.cdap.data.hbase.HBase96Test";
  }

  @Override
  protected String getHBase98Classname() {
    return "co.cask.cdap.data.hbase.HBase98Test";
  }

  @Override
  protected String getHBase10Classname() {
    return "co.cask.cdap.data.hbase.HBase10Test";
  }

  @Override
  protected String getHBase10CDHClassname() {
    return "co.cask.cdap.data.hbase.HBase10CDHTest";
  }

  @Override
  protected String getHBase11Classname() {
    return "co.cask.cdap.data.hbase.HBase11Test";
  }

  @Override
  protected String getHBase10CHD550ClassName() {
    return "co.cask.cdap.data.hbase.HBase10CDH550Test";
  }

  @Override
  protected String getHBase12CHD570ClassName() {
    return "co.cask.cdap.data.hbase.HBase12CDH570Test";
  }
}
