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

import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';

export function convertHistoryToDml(historyList) {
  let dmlArray = [];

  historyList.forEach((history) => {
    dmlArray.push(convertHistoryItemToDml(history));
  });

  return dmlArray;
}

function convertHistoryItemToDml(history) {
  let payload = history.payload;

  switch (history.action) {
    case WranglerActions.renameColumn:
      return `rename ${payload.activeColumn} ${payload.newName}`;
    case WranglerActions.dropColumn:
      return `drop ${payload.activeColumn}`;
    case WranglerActions.mergeColumn:
      return `merge ${payload.activeColumn} ${payload.mergeWith} ${payload.mergedColumnName} ${payload.joinBy}`;
    case WranglerActions.upperCaseColumn:
      return `uppercase ${payload.activeColumn}`;
    case WranglerActions.lowerCaseColumn:
      return `lowercase ${payload.activeColumn}`;
    case WranglerActions.titleCaseColumn:
      return `titlecase ${payload.activeColumn}`;
    case WranglerActions.subStringColumn:
      return `indexsplit ${payload.activeColumn} ${payload.beginIndex} ${payload.endIndex} ${payload.columnName}`;
    case WranglerActions.splitColumn:
      return `split ${payload.activeColumn} ${payload.delimiter} ${payload.firstSplit} ${payload.secondSplit}`;

  }
}
