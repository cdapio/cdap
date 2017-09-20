/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.spark.app;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Throwables;

/**
 * A dummy app with spark program in scala which counts the characters in a string
 */
public class SparkAppUsingObjectStore extends AbstractApplication {
  @Override
  public void configure() {
    try {
      createDataset("count", KeyValueTable.class);
      createDataset("totals", Table.class);

      ObjectStores.createObjectStore(getConfigurer(), "keys", String.class);

      addSpark(new CharCountProgram());
      addSpark(new ScalaCharCountProgram());
      addSpark(new ScalaCrossNSProgram());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }
}
