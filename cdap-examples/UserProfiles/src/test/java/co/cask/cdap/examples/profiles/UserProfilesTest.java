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

package co.cask.cdap.examples.profiles;

import co.cask.cdap.test.TestBase;
import org.junit.Test;

/**
 * Tests the UserProfiles example app.
 */
public class UserProfilesTest extends TestBase {

  @Test
  public void testUserProfiles() {
    // deploy the app
    // run the service and the flow
    // create a user through the service
    // read the user through the dataset
    // send a login event
    // verify the login time through the dataset
    // send an event to the stream
    // wait for flow to process the event
    // verify the last active time for the user
    // stop the service and the flow
  }

}
