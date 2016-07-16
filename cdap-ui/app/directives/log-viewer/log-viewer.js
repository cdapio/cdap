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

function LogViewerController ($scope, LogViewerStore, myLogsApi, LOGVIEWERSTORE_ACTIONS, MyCDAPDataSource) {
  'ngInject';

  this.data = {};
  this.errorCount = 0;
  this.warningCount = 0;
  this.loading = false;
  this.loadingMoreLogs = false;
  var dataSrc = new MyCDAPDataSource($scope);
  var pollPromise;

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

  //viewLimit and cacheDecrement should match
  this.viewLimit = 50;
  this.cacheDecrement = 50;
  this.cacheSize = 0;

  //Collapsing LogViewer Table Columns
  var theColumns = [];
  var cols = this.configOptions;
  var collapseCount = 0;

  if(cols['source']){
    theColumns.push('source');
  }
  if(cols['level']){
    theColumns.push('level');
  }
  if(cols['time']){
    theColumns.push('time');
  }

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

  let numEvents = 0;
  this.toggleExpandAll = false;

  LogViewerStore.subscribe(() => {
    this.logStartTime = LogViewerStore.getState().startTime;
    this.startTimeSec = Math.floor(this.logStartTime.getTime()/1000);
    this.loadingMoreLogs = true;
    requestWithStartTime();
  });

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

  this.updateScrollPositionInStore = function(val) {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION,
      payload: {
        scrollPosition: val
      }
    });
  };

  const requestWithOffset = () => {

    if(pollPromise){
      console.log('Stopping polling!!!!');
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }

    myLogsApi.nextLogsJsonOffset({
      'namespace' : this.namespaceId,
      'appId' : this.appId,
      'programType' : this.programType,
      'programId' : this.programId,
      'runId' : this.runId,
      'fromOffset' : this.fromOffset
    }).$promise.then(
      (res) => {

        this.fromOffset = res[res.length-1].offset;
        this.totalCount += res.length;

        if(res.length === 0){
          this.loadingMoreLogs = false;
          getStatus();
          return;
        }

        angular.forEach(res, (element, index) => {
          if(res[index].log.logLevel === 'WARN'){
            this.warningCount++;
          } else if(res[index].log.logLevel === 'ERROR'){
            this.errorCount++;
          }

          //Format dates properly for rendering and computing
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = ((formattedDate.getMonth() + 1) + '/' + formattedDate.getDate() + '/' + formattedDate.getFullYear() + ' ' + formattedDate.getHours() + ':' + ((formattedDate.getMinutes()<10) ? '0'+formattedDate.getMinutes() : formattedDate.getMinutes()) + ':' + formattedDate.getSeconds());
        });

        this.data = this.data.concat(res);
        this.cacheSize = res.length - this.cacheDecrement;
      },
      (err) => {
        console.log('ERROR: ', err);
      });
  };

  const getStatus = () => {
      myLogsApi.getLogsMetadata({
        'namespace' : this.namespaceId,
        'appId' : this.appId,
        'programType' : this.programType,
        'programId' : this.programId,
        'runId' : this.runId
      }).$promise.then(
        (statusRes) => {
          if(statusRes.status === 'RUNNING'){
            console.log('Polling for new logs!');
            pollForNewLogs();
          }
        },
        (statusErr) => {
          console.log('ERROR: ', statusErr);
        }
      );
  };

  const pollForNewLogs = () => {
    pollPromise = dataSrc.poll({
      _cdapPath: '/namespaces/' + this.namespaceId + '/apps/' + this.appId + '/flows/' + this.programType + '/runs/' + this.runId + '/logs?format=json&start=' + this.startTimeSec,
      method: 'GET'
    },
    function(res) {
      if(res.length > 0){
        console.log('Stopping polls!!!!');
        this.data = res;
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      }
    }, function(err){
      console.log('ERROR: ', err);
    });
  };

  const requestWithStartTime = () => {
    if(pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      console.log('If the current poll is activated, stop since we are updating start time');
      pollPromise = null;
    }

   myLogsApi.getLogsStart({
      'namespace' : this.namespaceId,
      'appId' : this.appId,
      'programType' : this.programType,
      'programId' : this.programId,
      'runId' : this.runId,
      'start' : this.startTimeSec
    }).$promise.then(
      (res) => {

        this.viewLimit = 50;
        this.cacheDecrement = 50;
        this.cacheSize = 0;

        //If there are no logs yet to be retrieved let viewer know
        if(res.length < this.viewLimit){
          this.loadingMoreLogs = false;
        } else {
          this.loadingMoreLogs = true;
        }
        //There are no more logs to be returned
        if(res.length === 0){
        //if(res.length < this.viewLimit) { //|| res.length < this.viewLimit){
          this.loadingMoreLogs = false;
          getStatus();
          return;
        }
        //clear the current array
        this.data.length = 0;

        // console.log('this is the new response:')
        this.fromOffset = res[res.length-1].offset;
        this.totalCount = res.length;
        this.warningCount = 0;
        this.errorCount = 0;
        angular.forEach(res, (element, index) => {
          if(res[index].log.logLevel === 'WARN'){
            this.warningCount++;
          } else if(res[index].log.logLevel === 'ERROR'){
            this.errorCount++;
          }
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = formatDate(formattedDate);
        });
        this.data = res;
        this.cacheSize = res.length - this.cacheDecrement;
      },
      (err) => {
        console.log('ERROR: ', err);
      });
  };

  function formatDate(date) {
    let month = date.getMonth() + 1;
    let day = date.getDate();
    let year = date.getFullYear();
    let hours = date.getHours();
    let minutes = date.getMinutes();
    let seconds = date.getSeconds();

    if(minutes < 10){
      minutes = '0' + minutes.toString();
    }
    if(hours < 10){
      hours = '0' + hours.toString();
    }
    if(seconds < 10){
      seconds = '0' + seconds.toString();
    }
    return month + '/' + day + '/' + year + ' ' + hours + ':' + minutes + ':' + seconds;
  }

  this.toggleLogExpansion = function() {
    this.toggleExpandAll = !this.toggleExpandAll;
    angular.forEach(this.data, (data) => {
      if(data.log.stackTrace.length > 0){
        data.isStackTraceExpanded = this.toggleExpandAll;
      }
    });
  };

  this.includeEvent = function(eventType){
    if(included[eventType]){
      numEvents--;
    } else{
      numEvents++;
    }
    included[eventType] = !included[eventType];
  };

  this.eventFilter = function(entry){
    if(numEvents === 0 || included[entry.log.logLevel]){
      return entry;
    }
    return;
  };

  this.scrollFn = _.debounce(function(){
    this.loadingMoreLogs = true;
    this.cacheSize -= this.cacheDecrement;
    if(this.cacheSize <= 0){
      requestWithOffset();
    }
    // computePinPosition();
    this.viewLimit += this.cacheDecrement;
  }, 1000);

  this.filterByStartDate = (entry) => {
    if(this.logStartTime > entry.log.timestamp) {
      return;
    }
    return entry;
  };
}

angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function () {
    return {
      templateUrl: 'log-viewer/log-viewer.html',
      controller: LogViewerController,
      scope: {
        displayOptions: '=?',
        namespaceId: '@',
        appId: '@',
        programType: '@',
        programId: '@',
        runId: '@'
      },
      bindToController: true,
      controllerAs: 'LogViewer'
    };
  });
