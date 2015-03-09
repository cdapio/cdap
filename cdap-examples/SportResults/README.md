# Sports example

Example application that illustrates PartitionedFileSet datasets using sport results.

Features introduced: Partitioned file sets, MapReduce with runtime arguments, Ad-hoc queries over file sets.

- Uses a partitioned file set to store game results. It is partitioned by league and season, and each partition
  is a file containing the results in one league for a season; for example, the 2014 season of the NFL
  (National Football League).
- Results are uploaded into the file set using a service.
- The results can be explored using ad-hoc SQL queries.
- A MapReduce program reads all results for a league and aggregates total counts across all seasons, and writes
  these to another partitioned table that is partitioned only by league.
- The totals can also be queried using ad-hoc SQL.

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
