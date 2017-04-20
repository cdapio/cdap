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

package co.cask.cdap.master.startup;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import org.junit.Test;

/**
 * Unit-tests for {@link MessagingSystemCheck}.
 */
public class MessagingSystemCheckTest {

  @Test
  public void testValid() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    new MessagingSystemCheck(cConf).run();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidTopic() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "invalid.topic");
    new MessagingSystemCheck(cConf).run();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidTopicNumber() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "topic:xyz");
    new MessagingSystemCheck(cConf).run();
  }

  @Test (expected = IllegalArgumentException.class)
  public void testNegativeTopicNumber() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.SYSTEM_TOPICS, "topic:-10");
    new MessagingSystemCheck(cConf).run();
  }
}
