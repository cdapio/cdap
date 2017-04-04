/*
 * Copyright © 2017 Cask Data, Inc.
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
/**
 * Maps a pipeline run status from the backend to display status on the frontend.
 */
angular.module(PKG.name + '.feature.hydrator')
  .factory('MyPipelineStatusMapper', function() {

    var statusMap = {
      'DEPLOYED': 'Deployed',
      'SUBMITTING': 'Submitting',
      'RUNNING': 'Running',
      'SUCCEEDED': 'Succeeded',
      'FAILED': 'Failed',
      'DRAFT': 'Draft',
      'STOPPED': 'Stopped',
      'COMPLETED': 'Succeeded',
      'KILLED': 'Stopped',
      'KILLED_BY_TIMER': 'Succeeded',
      'DEPLOY_FAILED': 'Failed',
      'RUN_FAILED': 'Failed',
      'SUSPENDED': 'Deployed',
      'SCHEDULED': 'Scheduled'
    };

    function lookupDisplayStatus (systemStatus) {
      if (systemStatus in statusMap) {
        return statusMap[systemStatus];
      } else {
        return systemStatus;
      }
    }

    return {
      lookupDisplayStatus: lookupDisplayStatus
    };
  });
