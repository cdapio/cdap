# User Profiles Example

An example application that manages user profiles in a Table. The fields of a user
profile are updated in different ways:

- Attributes such as name and email address are changed through a
  RESTful call when the user updates their profile;
- The time of the last login is updated by a sign-on service every
  time the user logs in, also through a RESTful call; and
- The time of the last activity is updated by a flow that processes
  events whenever it encounters an event from that user.

This application illustrates Column-Level Conflict Detection for a table.
A conflict occurs if two transactions that overlap in time modify the same data in a table.
For example, a flowlet's process method might overlap with a service handler.
Such a conflict is detected at the time that the transactions are committed,
and the transaction that attempts to commit last is rolled back.

- By default, the granularity of the conflict detection is at the row level.
  That means it is sufficient for two overlapping transactions writing to
  the same row of a table to cause a conflict, even if they write to different
  columns.
- Specifying a conflict detection level of COLUMN means that a conflict is
  only detected if both transactions modify the same column of the same row.
  This is more precise, but it requires more book-keeping in the transaction
  system and can thus impact performance.

Column-level conflict detection should be enabled if it is known that different
transactions frequently modify different columns of the same row concurrently.

This application uses:

- a stream ``events`` to receive events of user activity;
- a dataset ``profiles`` to store user profiles with conflict detection at the column level;
- a dataset ``counters`` to count events by URL (this is not essential for the purpose of the example);
- a service ``UserProfileService`` to create, delete, and update profiles; and
- a flow ``ActivityFlow`` to count events and record the time of last activity for the users.

To run the example, build and deploy the application, then start both the flow and the service.
Populate the ``profiles`` tables with users using the script ``bin/add-users.sh``. Now, from two
different terminals, run the following concurrently:

- ``bin/update-login.sh`` to randomly update the time of last login for users; and
- ``bin/send-events.sh`` to generate random user activity events and send them to the stream.

If both scripts are running at the same time, then some user profiles will be updated at the same by the
service and by the flow. With row-level conflict detection, you would see transaction conflicts in
the logs. But because the ``profiles`` table uses column level conflict detection, these conflicts
are avoided.

If you are interested to see the behavior with row-level conflict detection, you can change
the dataset creation statement at the bottom of``UserProfiles.java`` to use ``ConflictDetection.ROW``
and run the same steps as above. You should see transaction conflicts in the logs. For example, such
a conflict would shows as:

```
2015-01-26 21:00:20,084 - ERROR [FlowletProcessDriver-updater-0-executor:c.c.c.i.a.r.f.FlowletProcessDriver@279] - Transaction operation failed: Conflict detected for transaction 1422334820080000000.
co.cask.tephra.TransactionConflictException: Conflict detected for transaction 1422334820080000000.
	at co.cask.tephra.TransactionContext.checkForConflicts(TransactionContext.java:166) ~[tephra-core-0.3.4.jar:na]
	at co.cask.tephra.TransactionContext.finish(TransactionContext.java:78) ~[tephra-core-0.3.4.jar:na]
```

Note that in order to see this happen, you need to delete the ``profiles`` dataset before redeploying
the application, to force its recreation with the new properties.

For more information on running CDAP examples, see
http://docs.cask.co/cdap/current/en/examples-manual/examples/index.html.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Copyright © 2015 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.
