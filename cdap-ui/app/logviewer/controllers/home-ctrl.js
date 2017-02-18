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

class LogsAppHomeController {
  constructor($state, LogViewerStore, $scope, moment) {
    'ngInject';

    this.LogViewerStore = LogViewerStore;
    this.moment = moment;

    let {
      namespace,
      appId,
      programType,
      programId,
      runId,
    } = $state.params;

    this.namespace = namespace;
    this.appId = appId;
    this.programType = programType;
    this.programId = programId;
    this.runId = runId;

    this.getStatusInfo();
    this.sub = LogViewerStore.subscribe(this.getStatusInfo.bind(this));

    $scope.$on('$destroy', () => {
      this.sub();
    });
  }

  getStatusInfo() {
    let statusInfo = this.LogViewerStore.getState().statusInfo;
    this.startTime = statusInfo.startTime;
    this.endTime = statusInfo.endTime;
    this.status = statusInfo.status;
    if (document.title.indexOf('started at') === -1 && this.startTime) {
      document.title = document.title + ' (started at ' + this.moment.utc(this.startTime * 1000).format('MM/DD/YYYY HH:mm:ss')+ ' )';
    }
  }

  checkValidQueryParams() {
    if (
      !this.namespace ||
      !this.appId ||
      !this.programType ||
      !this.programId ||
      !this.runId
    ) {
      return false;
    }

    return true;
  }
}

angular.module(PKG.name + '.feature.logviewer')
.controller('LogsAppHomeController', LogsAppHomeController);
