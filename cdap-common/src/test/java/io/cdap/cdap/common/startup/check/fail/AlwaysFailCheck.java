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

package co.cask.cdap.common.startup.check.fail;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.startup.Check;
import com.google.inject.Inject;

/**
 * Check that always throws an exception with a configurable message. Used to check injection.
 */
public class AlwaysFailCheck extends Check {
  public static final String FAILURE_MESSAGE_KEY = "cdap.test.check.failure.key";
  public static final String NAME = "failure";
  private final CConfiguration conf;

  @Inject
  private AlwaysFailCheck(CConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public void run() throws Exception {
    throw new Exception(conf.get(FAILURE_MESSAGE_KEY, "I always fail."));
  }

  @Override
  public String getName() {
    return NAME;
  }
}
