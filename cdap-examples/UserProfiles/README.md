# User Profiles Example

Example application that manages user profiles in a Table. The fields of a user
profile are updated in different ways:

- Attributes like name and email address are changed through a REST
  call when the user updates his profile;
- The time of the last login is updated by a sign-on service every
  time the user logs in, also through a REST call;
- The time of the last activity is updated by a flow that processes
  events whenever it encounters an event from that user.

Features introduced: Column-Level Conflict Detection.

This application uses a table with conflict detection at the column level.
A conflict occurs if two transactions (for example, a flowlet's process method
and a service handler) that overlap in time modify the same data in a table.
Such a conflict is detected at the time that the transactions are committed,
and the transaction that attempts to commit last is rolled back.

- By default, the granularity of the conflict detection is at the row level.
  That means, it is sufficient for two overlapping transactions to write to
  the same row of a table to cause a conflict, even if they write different
  columns.
- Specifying a conflict detection level of COLUMN means that a conflict is
  only detected if both transactions modify the same column of the same row.
  This is more precise but also requires more book-keeping in the transaction
  system and therefore can impact performance.

Column-level conflict detection should be enabled if it is known that different
transactions can modify different columns of the same rows.

This application uses:

- A stream ``events`` to receive events of user activity;
- A dataset ``profiles`` to store user profiles with conflict detection at the column level;
- A dataset ``counters`` to count events by URL (this is not essential for the purpose of the example);
- A service ``UserProfileService`` to create, delete, and update profiles;
- A flow ``ActivityFlow`` to count events and also maintain the time of last activity for the users.

To run the example, build and deploy the application, then start both the flow and the service. Now
populate the ``profiles`` tables with users using the script ``bin/add-users.sh``. Now, from two
different terminals, run the following concurrently:

- ``bin/update-login.sh`` to randomly update the time of last login for users;
- ``bin/send-events.sh`` to generate random user activity events and send them to the stream.

If both scripts are running at the same, then some user profiles will be updated at the same by the
service and by the flow. With row-level conflict detection, you would see transaction conflicts in
the logs. But because the ``profiles`` table uses column level conflict detection, these conflicts
are avoided.

Note: If you are interested to see the behavior with row-level conflict detection, you can change
the dataset creation statement at the bottom of``UserProfiles.java`` to use ``ConflictDetection.ROW``
and run the same steps as above. You should see transaction conflicts in the logs.

For more information, see http://docs.cask.co/cdap/current/examples.


Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
