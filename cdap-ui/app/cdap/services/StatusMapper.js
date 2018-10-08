/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import {PROGRAM_STATUSES} from 'services/global-constants';

const statusMap = {
  [PROGRAM_STATUSES.DEPLOYED]: 'Deployed',
  [PROGRAM_STATUSES.SUBMITTING]: 'Submitting',
  [PROGRAM_STATUSES.RUNNING]: 'Running',
  [PROGRAM_STATUSES.SUCCEEDED]: 'Succeeded',
  [PROGRAM_STATUSES.FAILED]: 'Failed',
  [PROGRAM_STATUSES.DRAFT]: 'Draft',
  [PROGRAM_STATUSES.STOPPED]: 'Stopped',
  [PROGRAM_STATUSES.COMPLETED]: 'Succeeded',
  [PROGRAM_STATUSES.KILLED]: 'Stopped',
  [PROGRAM_STATUSES.KILLED_BY_TIMER]: 'Succeeded',
  [PROGRAM_STATUSES.DEPLOY_FAILED]: 'Failed',
  [PROGRAM_STATUSES.RUN_FAILED]: 'Failed',
  [PROGRAM_STATUSES.SUSPENDED]: 'Deployed',
  [PROGRAM_STATUSES.SCHEDULED]: 'Scheduled',
  [PROGRAM_STATUSES.STARTING]: 'Starting',
  [PROGRAM_STATUSES.SCHEDULING]: 'Scheduling',
  [PROGRAM_STATUSES.STOPPING]: 'Stopping',
  [PROGRAM_STATUSES.SUSPENDING]: 'Suspending',
  [PROGRAM_STATUSES.PENDING]: 'Provisioning',
};

function lookupDisplayStatus (systemStatus) {
  if (systemStatus in statusMap) {
    return statusMap[systemStatus];
  } else {
    return systemStatus;
  }
}

function getStatusIndicatorClass (displayStatus) {
  if (
    displayStatus === statusMap[PROGRAM_STATUSES.RUNNING] ||
    displayStatus === statusMap[PROGRAM_STATUSES.STARTING] ||
    displayStatus === statusMap[PROGRAM_STATUSES.PENDING]
  ) {
    return 'status-blue';
  } else if (
    displayStatus === statusMap[PROGRAM_STATUSES.SUCCEEDED] ||
    displayStatus === statusMap[PROGRAM_STATUSES.SCHEDULING] ||
    displayStatus === statusMap[PROGRAM_STATUSES.STOPPING]
  ) {
    return 'status-light-green';
  } else if (displayStatus === statusMap[PROGRAM_STATUSES.FAILED]) {
    return 'status-light-red';
  } else {
    return 'status-light-grey';
  }
}

function getStatusIndicatorIcon (displayStatus) {
  if (
    displayStatus === statusMap[PROGRAM_STATUSES.PENDING] ||
    displayStatus === statusMap[PROGRAM_STATUSES.DRAFT]
  ) {
    return 'icon-circle-o';
  }
  return 'icon-circle';
}

export default {
  statusMap,
  lookupDisplayStatus,
  getStatusIndicatorClass,
  getStatusIndicatorIcon,
};
