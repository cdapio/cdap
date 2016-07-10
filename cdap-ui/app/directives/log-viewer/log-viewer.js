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

function LogViewerController ($scope, $resource, LogViewerStore, myLogsApi) {
  'ngInject';

  //include above when no longer hard coding: $stateParams
  // var namespace = 'default',
  //     appId = 'HelloWorld',
  //     programType = 'flows',
  //     programId = 'WhoFlow',
  //     runId = '3d7bef02-453e-11e6-8c94-56219b501a22';

  var namespace = 'default',
      appId = 'PurchaseHistory',
      programType = 'flows',
      programId = 'PurchaseFlow',
      runId = '53522903-46e4-11e6-b2ff-56219b501a22',
      pollPromise = null;

  this.data = {};

  myLogsApi.nextLogsJson({
    'namespace' : namespace,
    'appId' : appId,
    'programType' : programType,
    'programId' : programId,
    'runId' : runId,
    'start' : -10000.1468004430508
  }).$promise.then(
    (res) => {
      console.log('success from logviewer controller');
      //Process the data
      angular.forEach(res, (element, index) => {
        let formattedDate = new Date(res[index].log.timestamp);
        res[index].log.stackTrace = 'test';
        console.log('Testing the date: ' + formattedDate);
        res[index].log.timestamp = formattedDate;
        res[index].log.displayTime = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + formattedDate.getMinutes() + ':' + formattedDate.getSeconds());
      });
      this.data = res;
    },
    (err) => {
      console.log('ERROR: ', err);
    });

  LogViewerStore.subscribe(() => {
    this.logStartTime = LogViewerStore.getState().startTime;
  });

  this.configOptions = {
    time: true,
    level: true,
    source: true,
    message: true
  };

  this.hiddenColumns = {
    time: false,
    level: false,
    source: false,
    message: false
  };

  var theColumns = [];
  var cols = this.configOptions;

  if(cols['source']){
    theColumns.push('source');
  }
  if(cols['level']){
    theColumns.push('level');
  }
  if(cols['time']){
    theColumns.push('time');
  }

  var collapseCount = 0;
  this.collapseColumns = () => {
    if(this.isMessageExpanded){
      this.isMessageExpanded = !this.isMessageExpanded;
    }
    if(collapseCount < theColumns.length){
      this.hiddenColumns[theColumns[collapseCount++]] = true;
      if(collapseCount === theColumns.length){
        this.isMessageExpanded = true;
      }
    } else {
      collapseCount = 0;
      for(var key in this.hiddenColumns){
        if(this.hiddenColumns.hasOwnProperty(key)){
          this.hiddenColumns[key] = false;
        }
      }
    }
  };

  angular.forEach($scope.displayOptions, (value, key) => {
    this.configOptions[key] = value;
  });

  this.logEvents = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

  let included = {
    'ERROR' : false,
    'WARN' : false,
    'INFO' : false,
    'DEBUG' : false,
    'TRACE' : false
  };

  // this.totalCount = this.data.length;
  let errorCount = 0;
  let warningCount = 0;
  let numEvents = 0;

  //Compute Total
  for(let k = 0; k < this.data.length; k++){
    let currentItem = this.data[k].level;
    if(currentItem === 'ERROR'){
      errorCount++;
    } else if(currentItem === 'WARN'){
      warningCount++;
    }
  }

  this.errorCount = errorCount;
  this.warningCount = warningCount;
  this.toggleExpandAll = false;

  this.toggleLogExpansion = function() {
    this.toggleExpandAll = !this.toggleExpandAll;
    angular.forEach(this.data, (data) => {
      data.isStackTraceExpanded = this.toggleExpandAll;
    });
  };

  //New 'includeEvent' function
  this.includeEvent = function(eventType){
    if(included[eventType]){
      numEvents--;
    } else{
      numEvents++;
    }
    included[eventType] = !included[eventType];
  };

  this.eventFilter = function(log){
    if(numEvents === 0 || included[log.level]){
      return log;
    }
    return;
  };

  this.filterByStartDate = (log) => {
    if(this.logStartTime > log.time) {
      return;
    }
    return log;
  };
}

angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function () {
    return {
      templateUrl: 'log-viewer/log-viewer.html',
      controller: LogViewerController,
      scope: {
        displayOptions: '=?'
      },
      controllerAs: 'LogViewer'
    };
  });
