/*
 * Copyright © 2020 Cask Data, Inc.
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

import { List, Map } from 'immutable';

import { IRequestHistory } from 'components/HttpExecutor/RequestHistoryTab';
import moment from 'moment';

export function getDateID(date: Date) {
  return moment(date).format('dddd, MMMM D, YYYY');
}

export function getRequestsByDate(log: Map<string, List<IRequestHistory>>, dateID: string) {
  return log.get(dateID) || List([]);
}

export function compareByTimestamp(a: string, b: string) {
  const timestampA = new Date(a);
  const timestampB = new Date(b);
  if (timestampA < timestampB) {
    return 1;
  } else if (timestampA > timestampB) {
    return -1;
  } else {
    return 0;
  }
}
