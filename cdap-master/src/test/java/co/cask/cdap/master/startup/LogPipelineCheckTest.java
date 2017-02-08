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
import co.cask.cdap.logging.framework.InvalidPipelineException;
import org.junit.Test;

/**
 *
 */
public class LogPipelineCheckTest {

  @Test
  public void testValid() throws Exception {
    new LogPipelineCheck(CConfiguration.create()).run();
  }

  @Test (expected = InvalidPipelineException.class)
  public void testInvalid() throws Exception {
    // Sets an invalid config for the CDAPLogAppender
    CConfiguration cConf = CConfiguration.create();
    cConf.set("log.pipeline.cdap.file.sync.interval.bytes", "-1");

    new LogPipelineCheck(cConf).run();
  }
}
