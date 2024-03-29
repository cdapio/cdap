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

package io.cdap.cdap.data.startup;

import io.cdap.cdap.common.conf.CConfiguration;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class TransactionServiceCheckWithHConfTest extends TransactionServiceCheckTest {

  protected void run(CConfiguration cConf) throws Exception {
    Configuration hConf = new Configuration();
    for (Map.Entry<String, String> entry : cConf) {
      hConf.set(entry.getKey(), entry.getValue());
    }
    new TransactionServiceCheck(hConf).run();
  }
}
