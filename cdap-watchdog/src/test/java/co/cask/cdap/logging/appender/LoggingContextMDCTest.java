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

package co.cask.cdap.logging.appender;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.logback.TestLoggingContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class LoggingContextMDCTest {

  @Test
  public void testMDC() {
    LoggingContext context = new TestLoggingContext("namespace", "app", "run", "instance");

    // Put an entry in the user mdc. It shouldn't override what's in the system tags.
    Map<String, String> userMDC = new HashMap<>();
    userMDC.put(Constants.Logging.TAG_APPLICATION_ID, "userApp");

    Map<String, String> mdc = new LoggingContextMDC(context, userMDC);

    Assert.assertEquals(4, mdc.size());

    Map<String, String> copiedMDC = new HashMap<>();
    for (Map.Entry<String, String> entry : mdc.entrySet()) {
      copiedMDC.put(entry.getKey(), entry.getValue());
    }

    Assert.assertEquals(4, copiedMDC.size());
    Assert.assertEquals("namespace", copiedMDC.get(Constants.Logging.TAG_NAMESPACE_ID));
    Assert.assertEquals("app", copiedMDC.get(Constants.Logging.TAG_APPLICATION_ID));
    Assert.assertEquals("run", copiedMDC.get(Constants.Logging.TAG_RUN_ID));
    Assert.assertEquals("instance", copiedMDC.get(Constants.Logging.TAG_INSTANCE_ID));

    // Should be able to set user property
    mdc.put("user", "test");
    Assert.assertEquals(5, mdc.size());
    Assert.assertEquals(5, mdc.entrySet().size());

    // This should fail with exception
    try {
      mdc.put(Constants.Logging.TAG_APPLICATION_ID, "newApp");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
