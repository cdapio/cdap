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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Tables;

/**
 * Demonstrates the use of column-level conflict detection by example of managing user profiles,
 * where attributes like name and email address avn be updated without conflicting with updates
 * to the last active time of the user.
 */
public class UserProfiles extends AbstractApplication {

  @Override
  public void configure() {
    setName("UserProfiles");
    setDescription("Demonstrates the use of column-level conflict detection.");
    addService(new UserProfileService());
    addFlow(new ActivityFlow());
    addStream(new Stream("events"));
    createDataset("counters", KeyValueTable.class);
    createDataset("profiles", "table",
                  Tables.tableProperties(ConflictDetection.COLUMN,  // column level
                                         -1,                        // no time-to-live
                                         DatasetProperties.EMPTY)); // no extra properties
  }
}
