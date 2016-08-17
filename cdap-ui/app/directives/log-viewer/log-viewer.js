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

function LogViewerController ($scope, LogViewerStore, myLogsApi, LOGVIEWERSTORE_ACTIONS, MyCDAPDataSource, $sce, myCdapUrl, $timeout, $uibModal, $q) {
  'ngInject';

  var dataSrc = new MyCDAPDataSource($scope);
  var pollPromise;
  //Collapsing LogViewer Table Columns
  var columnsList = [];
  var collapseCount = 0;
  this.$uibModal = $uibModal;
  var rawLogs = {
    log: '',
    startTime: ''
  };

  this.setProgramMetadata = (status) => {
    this.programStatus = status;

    if(!this.entityName) {
      this.entityName = this.programId;
    }

    switch(status){
      case 'RUNNING':
      case 'STARTED':
        this.statusType = 0;
        break;
      case 'STOPPED':
      case 'KILLED':
      case 'FAILED':
      case 'SUSPENDED':
        this.statusType = 1;
        break;
      case 'COMPLETED':
        this.statusType = 2;
        break;
      default:
        this.statusType = 3;
        break;
    }
  };

  this.setDefault = () => {
    this.textFile = null;
    this.statusType = 3;
    this.displayData = [];
    this.data = [];
    this.loading = false;
    this.errorCount = 0;
    this.warningCount = 0;
    this.totalCount = 0;
    this.fullScreen = false;
    this.programStatus = 'Not Started';
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

  this.openRaw = () => {
    function RawLogsModalCtrl($scope, MyCDAPDataSource, rAppId, rProgramType, rProgramId, rRunId, rStartTimeSec, rIsApplicationRunning) {
      this.applicationName = rProgramId;
      this.startTime = formatDate(new Date(rStartTimeSec*1000));
      this.rawIsLoaded = false;
      this.noRawData = false;
      this.windowMode = 'regular';

      this.toggleMaximizedView = (isExpanded) => {
        this.windowMode = (isExpanded) ? 'expand' : 'regular';
      };
      let loadFromCache = !rIsApplicationRunning &&
       rawLogs &&
       rawLogs.startTime === rStartTimeSec &&
       rawLogs.log &&
       rawLogs.log.length > 0;
      if (loadFromCache) {
        this.rawDataResponse = rawLogs.log;
        this.rawIsLoaded = true;
        return;
      }
      var modalDataSrc = new MyCDAPDataSource($scope);
      modalDataSrc.request({
        _cdapNsPath: `/apps/${rAppId}/${rProgramType}/${rProgramId}/runs/${rRunId}/logs?start=${rStartTimeSec}`
      }).then((res) => {
        if(typeof res === 'undefined' || res.length === 0){
          this.noRawData = true;
        } else {
          this.rawDataResponse = res;
          if (!rIsApplicationRunning) {
            rawLogs = rawLogs || {};
            rawLogs.log = res;
            rawLogs.startTime = rStartTimeSec;
          }
          this.rawIsLoaded = true;
        }
      });
    }

    this.$uibModal.open({
      size: 'lg',
      windowTemplateUrl: 'log-viewer/raw-template.html',
      templateUrl: 'log-viewer/raw.html',
      windowClass: 'node-config-modal raw-modal cdap-modal',
      animation: false,
      controller: ['$scope', 'MyCDAPDataSource', 'rAppId', 'rProgramType', 'rProgramId', 'rRunId', 'rStartTimeSec', 'rIsApplicationRunning', RawLogsModalCtrl],
      controllerAs: 'RawLogsModalCtrl',
      resolve: {
        rAppId: () => {
          return this.appId;
        },
        rProgramType: () => {
          return this.programType;
        },
        rProgramId: () => {
         return this.programId;
        },
        rRunId: () => {
          return this.runId;
        },
        rStartTimeSec: () => {
          return this.startTimeSec;
        },
        rIsApplicationRunning: () => {
          return this.statusCode === 0;
        }
      }
    });
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

  let unsub = LogViewerStore.subscribe(() => {
    this.logStartTime = LogViewerStore.getState().startTime;
    if (typeof this.logStartTime !== 'object') {
      this.setDefault();
      return;
    }

    this.startTimeSec = Math.floor(this.logStartTime.getTime()/1000);
    requestWithStartTime();
  });

  if (this.runId) {
    //Get Initial Status
    myLogsApi.getLogsMetadata({
      namespace : this.namespaceId,
      appId : this.appId,
      programType : this.programType,
      programId : this.programId,
      runId : this.runId
    }).$promise.then(
      (statusRes) => {
        this.setProgramMetadata(statusRes.status);
      },
      (statusErr) => {
        console.log('ERROR: ', statusErr);
      });
  }

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

  this.toggleStackTrace = (index) => {
    //If the currently clicked row is a stack trace itself, do nothing
    if(this.displayData[index].stackTrace){
      return;
    }

    if( (index+1 < this.displayData.length) && this.displayData[index+1].stackTrace){
      this.displayData.splice(index+1, 1);
      this.displayData[index].selected = false;
      return;
    }
    if(this.displayData[index].log.stackTrace){
      this.displayData[index].selected = true;
      let stackTraceObj = {
        log: {
          timestamp: this.displayData[index].log.timestamp,
          stackTrace: this.displayData[index].log.stackTrace,
        },
        stackTrace: true
      };
      this.displayData.splice(index+1, 0, stackTraceObj);
    } else {
      //otherwise, it does not have stack trace but has been selected
      this.displayData[index].selected = !this.displayData[index].selected;
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
        this.renderData(true);
        if(this.displayData.length < this.viewLimit){
          getStatus();
        }

      },
      (err) => {
        console.log('ERROR: ', err);
      });
  };

  const getStatus = () => {
    myLogsApi.getLogsMetadata({
      namespace : this.namespaceId,
      appId : this.appId,
      programType : this.programType,
      programId : this.programId,
      runId : this.runId
    }).$promise.then(
      (statusRes) => {
        this.setProgramMetadata(statusRes.status);
        if(this.statusType === 0){
          if (!pollPromise) {
            pollForNewLogs();
          }
        } else {
          if (pollPromise) {
            dataSrc.stopPoll(pollPromise.__pollId__);
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
        this.renderData(true);
      }

      if(this.displayData.length > this.viewLimit){
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      } else {
        getStatus();
      }

    }, (err) => {
      console.log('ERROR: ', err);
    });
  };

  var exportTimeout = null;

  const downloadLogs = () => {
    if (this.statusType !== 0 && rawLogs && rawLogs.startTime === this.startTimeSec) {
      return $q.resolve(rawLogs.log);
    }
    return myLogsApi.getLogsStartAsRaw({
      namespace : this.namespaceId,
      appId : this.appId,
      programType : this.programType,
      programId : this.programId,
      runId : this.runId,
      start : this.startTimeSec
    })
      .$promise
      .then(
        (res) => {
          if (!this.isApplicationRunning) {
            rawLogs = {
              log: res,
              startTime: this.startTimeSec
            };
          }
          return res;
        },
        (err) => {
          this.isDownloading = false;
          console.log('ERROR: ', err);
        }
      );
  };

  this.export = () => {
    this.isDownloading = true;
    downloadLogs()
    .then((log) => {
      var blob = new Blob([log], {type: 'text/plain'});
      this.url = URL.createObjectURL(blob);
      let filename = '';
      if ('undefined' !== typeof this.getDownloadFilename()) {
        filename = this.getDownloadFilename() + '-' + formatDate(new Date(this.startTimeSec*1000), true);
      } else {
        filename = this.namespaceId + '-' + this.appId + '-' + this.programType + '-' + this.programId + '-' + formatDate(new Date(this.startTimeSec*1000), true);
      }
      this.exportFileName = filename;
      $scope.$on('$destroy', () => {
        URL.revokeObjectURL(this.url);
        $timeout.cancel(exportTimeout);
      });

      $timeout.cancel(exportTimeout);

      exportTimeout = $timeout(() => {
        document.getElementById('logs-export-link').click();
        this.isDownloading = false;
      });
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
      _cdapNsPath: `/apps/${this.appId}/${this.programType}/${this.programId}/runs/${this.runId}/logs?&start=${this.startTimeSec}`
    });

    this.rawUrl = url;

    myLogsApi.getLogsStartAsJson({
        namespace : this.namespaceId,
        appId : this.appId,
        programType : this.programType,
        programId : this.programId,
        runId : this.runId,
        start : this.startTimeSec
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

        this.data = res;
        if(res.length === 0){
          this.renderData();
          getStatus();
          return;
        }

        this.fromOffset = res[res.length-1].offset;
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

  function formatDate(date, isDownload) {
    let dateObj = {
      month: date.getMonth() + 1,
      day: date.getDate(),
      year: date.getFullYear(),
      hours: date.getHours(),
      minutes: date.getMinutes(),
      seconds: date.getSeconds()
    };

    angular.forEach(dateObj, (value, key) => {
      if(value < 10){
        dateObj[key] = '0' + value;
      } else {
        dateObj[key] = value.toString();
      }
    });

    if(isDownload){
      return dateObj.year + dateObj.day + dateObj.month + dateObj.hours + dateObj.minutes + dateObj.seconds;
    }

    return dateObj.month + '/' + dateObj.day + '/' + dateObj.year + ' ' + dateObj.hours + ':' + dateObj.minutes + ':' + dateObj.seconds;
  }

  this.toggleLogExpansion = function() {
    let len = this.displayData.length;
    this.toggleExpandAll = !this.toggleExpandAll;
    for(var i = 0 ; i < len ; i++) {
      let entry = this.displayData[i];
      if(!entry.stackTrace && entry.log.stackTrace.length > 0){
        entry.isStackTraceExpanded = this.toggleExpandAll;

        if(i < this.displayData.length && this.toggleExpandAll && (i+1 === this.displayData.length || !this.displayData[i+1].stackTrace)){
          this.displayData[i].selected = true;
          let stackTraceObj = {
            log: {
              timestamp: this.displayData[i].log.timestamp,
              stackTrace: this.displayData[i].log.stackTrace,
            },
            stackTrace: true
          };
          this.displayData.splice(i+1, 0, stackTraceObj);
          len++;
        } else if(!this.toggleExpandAll && !entry.stackTrace && i+1 < this.displayData.length && this.displayData[i+1].stackTrace){
          this.displayData[i].selected = false;
          this.displayData.splice(i+1, 1);
          len--;
        }
      } else {
        this.displayData[i].selected = this.toggleExpandAll;
      }
    }
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

  this.renderData = (renderNewFromOffset) => {
    //Clean slate

    if(!renderNewFromOffset){
      this.displayData = [];
      this.viewLimit = 100;
      this.cacheDecrement = 100;
      this.cacheSize = 0;
    }

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

  $scope.$on('$destroy', function() {
    if (unsub) {
      unsub();
    }
    if(pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }
  });
}

angular.module(PKG.name + '.commons')
  .directive('myLogViewer', function () {
    return {
      templateUrl: 'log-viewer/log-viewer.html',
      controller: LogViewerController,
      controllerAs: 'LogViewer',
      scope: {
        displayOptions: '=?',
        namespaceId: '@',
        appId: '@',
        programType: '@',
        programId: '@',
        runId: '@',
        getDownloadFilename: '&',
        entityName: '@'
      },
      bindToController: true
    };
  });
