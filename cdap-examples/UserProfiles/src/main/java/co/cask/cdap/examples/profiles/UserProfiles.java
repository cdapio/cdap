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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.ConflictDetection;
import co.cask.cdap.api.dataset.table.Tables;

/**
 * Demonstrates the use of column-level conflict detection by the example of managing user profiles,
 * where individual attributes such as name and email address can be updated without conflicting
 * with updates to other attributes such as the last active time of the user. This is achieved
 * by setting the conflict resolution level of the "profiles" table to COLUMN.
 */
public class UserProfiles extends AbstractApplication {

  @Override
  public void configure() {
    setName("UserProfiles");
    setDescription("Demonstrates the use of column-level conflict detection.");
    addStream(new Stream("events"));
    addFlow(new ActivityFlow());
    addService(new UserProfileService());
    createDataset("counters", KeyValueTable.class);
    // create the profiles table with column-level conflict detection
    Tables.createTable(getConfigurer(), "profiles", ConflictDetection.COLUMN);
  }
}
