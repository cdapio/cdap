/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.app.verification;

import co.cask.cdap.proto.id.ApplicationId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests AbstractVerifier class at function level
 */
public class AbstractVerifierTest {

  /**
   * Checking if name is an id or no.
   */
  @Test
  public void testId() throws Exception {
    AbstractVerifier<String> v = new AbstractVerifier<String>() {

      @Override
      protected String getName(String input) {
        return input;
      }
    };

    ApplicationId appId = new ApplicationId("test", "some");

    Assert.assertTrue(v.verify(appId, "foo").isSuccess());
    Assert.assertTrue(v.verify(appId, "mydataset").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo-name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo_name").isSuccess());
    Assert.assertTrue(v.verify(appId, "foo1234").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo^ name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo^name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo/name").isSuccess());
    Assert.assertFalse(v.verify(appId, "foo$name").isSuccess());
  }

}
