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

function LogViewerController ($scope, LogViewerStore, myLogsApi, LOGVIEWERSTORE_ACTIONS, MyCDAPDataSource, $sce, myCdapUrl) {
  'ngInject';

  var dataSrc = new MyCDAPDataSource($scope);
  var pollPromise;
  //Collapsing LogViewer Table Columns
  var columnsList = [];
  var collapseCount = 0;

  this.setDefault = () => {
    this.displayData = [];
    this.data = [];
    this.loading = false;
    this.errorCount = 0;
    this.warningCount = 0;
    this.totalCount = 0;
    this.fullScreen = false;
    this.applicationIsRunning = false;

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
    this.viewLimit = 100;
    this.cacheDecrement = 100;
    this.cacheSize = 0;
    var cols = this.configOptions;

    if(cols['source']){
      columnsList.push('source');
    }
    if(cols['level']){
      columnsList.push('level');
    }
    if(cols['time']){
      columnsList.push('time');
    }
  };

  this.setDefault();
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
    if (typeof this.logStartTime !== 'object') {
      this.setDefault();
      return;
    }
    this.startTimeSec = Math.floor(this.logStartTime.getTime()/1000);
    requestWithStartTime();
  });

  this.filterSearch = () => {
    //Rerender data
    this.renderData();
    //If the search query is blank, otherwise filter
    if(this.searchText.length === 0){
      return;
    }

    this.displayData = this.displayData.filter( data => {
      return data.log.message.toLowerCase().indexOf(this.searchText.toLowerCase()) !== -1;
    });
  };

  this.showStackTrace = (index) => {
    //If the stack trace is showing, remove it
    if( (index+1 < this.displayData.length) && this.displayData[index+1].stackTrace){
      this.displayData.splice(index+1, 1);
      this.displayData[index].selected = false;
      return;
    }
    //If the currently clicked row is a stack trace itself, do nothing
    else if(this.displayData[index].stackTrace && (index - 1) > 0){
      return;
    }

    if(this.displayData[index].log.stackTrace){
      this.displayData[index].selected = true;
      var stackTraceObj = JSON.parse(JSON.stringify(this.displayData[index]));
      stackTraceObj.stackTrace = true;
      this.displayData.splice(index+1, 0, stackTraceObj);
    }
  };

  this.collapseColumns = () => {
    if(this.isMessageExpanded){
      this.isMessageExpanded = !this.isMessageExpanded;
    }
    if(collapseCount < columnsList.length){
      this.hiddenColumns[columnsList[collapseCount++]] = true;
      if(collapseCount === columnsList.length){
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

        if(res.length === 0){
          getStatus();
          return;
        }

        this.fromOffset = res[res.length-1].offset;
        this.totalCount += res.length;

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
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        this.data = this.data.concat(res);
        this.cacheSize = res.length - this.cacheDecrement;
        this.renderData();
        if(res.length < this.viewLimit){
          getStatus();
        }

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
            this.applicationIsRunning = true;
            if (!pollPromise) {
              pollForNewLogs();
            }
          } else {
            this.applicationIsRunning = false;
            if (pollPromise) {
              pollPromise.stopPoll(pollPromise.__pollId__);
            }
          }
        },
        (statusErr) => {
          console.log('ERROR: ', statusErr);
        }
      );
  };

  const pollForNewLogs = () => {
    pollPromise = dataSrc.poll({
      _cdapPath: '/namespaces/' + this.namespaceId + '/apps/' + this.appId + '/' + this.programType + '/' + this.programId + '/runs/' + this.runId + '/logs?format=json&start=' + this.startTimeSec,
      method: 'GET'
    },
    (res) => {
      //We have recieved more logs, append to current dataset
      if(res.length > this.data.length){
        res = res.slice(this.data.length, res.length);

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
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        this.data = this.data.concat(res);
      }

      if(this.data.length > this.viewLimit){
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      }
      this.renderData();
      getStatus();

    }, (err) => {
      console.log('ERROR: ', err);
    });
  };

  const requestWithStartTime = () => {
    this.loading = true;
    if(pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }

    // FIXME: This should be provided by $resource or MyCdapResource. Thank you $resource & angular
    const url = myCdapUrl.constructUrl({
      _cdapNsPath: `/apps/${this.appId}/${this.programType}/${this.programId}/runs/${this.runId}/logs?format=json&start=${this.startTimeSec}`
    });

    this.rawUrl = url;

    myLogsApi.getLogsStart({
        'namespace' : this.namespaceId,
        'appId' : this.appId,
        'programType' : this.programType,
        'programId' : this.programId,
        'runId' : this.runId,
        'start' : this.startTimeSec
    }).$promise.then(
      (res) => {
        this.loading = false;
        this.viewLimit = 100;
        this.cacheDecrement = 100;
        this.cacheSize = 0;
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
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        if(res.length === 0){
          getStatus();
          return;
        }

        this.fromOffset = res[res.length-1].offset;
        this.data = res;
        this.renderData();
        this.cacheSize = res.length - this.cacheDecrement;

        if(res.length < this.viewLimit){
          getStatus();
        }
      },
      (err) => {
        this.setDefault();
        this.loading = false;
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
    angular.forEach(this.displayData, (entry, key) => {
      if(!entry.stackTrace && entry.log.stackTrace.length > 0){
        entry.isStackTraceExpanded = this.toggleExpandAll;

        if(this.toggleExpandAll && !this.displayData[key+1].stackTrace){
          this.displayData[key].selected = true;
          var stackTraceObj = JSON.parse(JSON.stringify(this.displayData[key]));
          stackTraceObj.stackTrace = true;
          this.displayData.splice(key+1, 0, stackTraceObj);
        } else if(!this.toggleExpandAll && !entry.stackTrace && key+1 < this.displayData.length && this.displayData[key+1].stackTrace){
          this.displayData[key].selected = false;
          this.displayData.splice(key+1, 1);
        }
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
    this.renderData();
  };

  this.renderData = () => {
    //Clean slate
    this.displayData = [];
    this.viewLimit = 100;
    this.cacheDecrement = 100;
    this.cacheSize = 0;

    if(numEvents === 0){
      angular.forEach(this.data, (value, key) => {
        this.displayData.push(this.data[key]);
      });
      return;
    } else {
      angular.forEach(this.data, (value, key) => {
        if(included[this.data[key].log.logLevel]){
          this.displayData.push(this.data[key]);
        }
      });
    }
  };
  this.highlight = (text) => {
    if(!this.searchText || (this.searchText && !this.searchText.length)){
     return $sce.trustAsHtml(text);
    }

    return $sce.trustAsHtml(
      text.replace(new RegExp(this.searchText, 'gi'),
      '<span class="highlighted-text">$&</span>'
    ));
  };

  this.eventFilter = function(entry){
    if(numEvents === 0 || included[entry.log.logLevel]){
      return entry;
    }
    return;
  };

  this.scrollFn = function(){
    this.cacheSize -= this.cacheDecrement;
    if(this.cacheSize <= 0){
      requestWithOffset();
    }
    this.viewLimit += this.cacheDecrement;
  };

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
