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

function LogViewerPreviewController ($scope, $window, LogViewerStore, myPreviewLogsApi, LOGVIEWERSTORE_ACTIONS, MyCDAPDataSource, $sce, myCdapUrl, $timeout, $q, moment, myAlertOnValium) {
  'ngInject';

  /**
   *  For reference:
   *  The entry point for this log is startTimeRequest()
   **/

  var dataSrc = new MyCDAPDataSource($scope);
  var pollPromise;
  //Collapsing LogViewer Table Columns
  var columnsList = [];
  var collapseCount = 0;
  var vm = this;

  vm.viewLimit = 100;
  vm.errorRetrievingLogs = false;

  vm.setProgramMetadata = (status) => {
    vm.programStatus = status;

    if(!vm.entityName) {
      vm.entityName = vm.runId;
    }

    switch(status){
      case 'fNING':
      case 'STARTED':
        vm.statusType = 0;
        break;
      case 'STOPPED':
      case 'KILLED':
      case 'FAILED':
      case 'SUSPENDED':
        vm.statusType = 1;
        break;
      case 'COMPLETED':
        vm.statusType = 2;
        break;
      default:
        vm.statusType = 3;
        break;
    }
  };
  let page = angular.element(window);

  vm.setDefault = () => {
    vm.textFile = null;
    vm.statusType = 3;
    vm.displayData = [];
    vm.data = [];
    vm.loading = false;
    vm.fullScreen = false;
    vm.programStatus = 'Not Started';
    vm.configOptions = {
      time: true,
      level: true,
      source: true,
      message: true
    };

    vm.hiddenColumns = {
      time: false,
      level: false,
      source: false,
      message: false
    };
    var cols = vm.configOptions;

    if(cols['source']){
      columnsList.push('source');
    }
    if(cols['level']){
      columnsList.push('level');
    }
    if(cols['time']){
      columnsList.push('time');
    }
    // FIXME: vm should have been defaulted from LogViewerStore but since we didn't plan for having a store for logviewer
    // we are having vm adhoc assignment.
    vm.totalCount = 0;
    vm.warningCount = 0;
    vm.errorCount = 0;
  };

  vm.setDefault();
  angular.forEach($scope.displayOptions, (value, key) => {
    vm.configOptions[key] = value;
  });

  vm.logEvents = ['ERROR', 'WARN', 'INFO', 'DEBUG', 'TRACE'];

  vm.activeLogLevels = {
    'ERROR' : true,
    'WARN' : true,
    'INFO' : true,
    'DEBUG' : false,
    'TRACE' : false
  };

  // dynamically sets the default log level filter
  vm.logEvents.forEach(logLevel => {
    if (vm.activeLogLevels[logLevel] === true) {
      vm.selectedLogLevel = logLevel;
    }
  });

  vm.toggleExpandAll = false;

  let unsub = LogViewerStore.subscribe(() => {
    let logViewerState = LogViewerStore.getState();

    vm.totalCount = logViewerState.totalLogs;
    vm.warningCount = logViewerState.totalWarnings;
    vm.errorCount = logViewerState.totalErrors;

    vm.fullScreen = logViewerState.fullScreen;

    if (vm.data.length > 0) {
      return;
    }
    // if (vm.logStartTime === logViewerState.startTime){
    //   return;
    // }

    // vm.logStartTime = logViewerState.startTime;

    // if (!vm.logStartTime || vm.startTimeMs === vm.logStartTime){
    //   return;
    // }

    // vm.startTimeMs = (vm.logStartTime instanceof Date) ? vm.logStartTime.getTime() : vm.logStartTime;

    // vm.fromOffset = -10000 + '.' + vm.startTimeMs;

    vm.endRequest = false;
    startTimeRequest();
  });

  let proximityVal = Number.MAX_VALUE;
  let newTime;
  let timeout;

  vm.inViewScrollUpdate = (index, isInview, event) => {

    if(isInview && vm.displayData && vm.displayData.length > 0) {

      // tbody extends beyond the viewport when scrolling down the table
      let topOfTable = event.inViewTarget.parentElement.getBoundingClientRect().top;

      // measures the scroll position of the window within the viewport
      let pageScrollPosition = page[0].scrollY;

      // gives the offsetTop property for a row that is within the viewport
      let rowTopVal = event.inViewTarget.offsetTop;

      // Adjusted val combines the tbody absolute offset that may extend beyond the viewport, with it's relatively positioned table row, giving us an absolute positioning for the row
      let adjustedVal = topOfTable + pageScrollPosition + rowTopVal;

      // Difference accounts for the difference between the top of the logviewer table container and the table rows
      let difference = adjustedVal - $scope.tableEl[0].offsetTop;

      // Offset the height of the bottom timeline and scrollpin row (55 + 15) if in full-screen
      if (vm.fullScreen){
        difference-=70;
      }

      // By taking the smallest non-negative value, we have found the top-most row
      if (difference > 0 && (proximityVal > difference || proximityVal < 0)){
        index++;
        newTime = vm.displayData[index].log.timestamp;
        vm.updateScrollPositionInStore(newTime);
      }

      proximityVal = difference;
    }
  };

  if (vm.previewId) {
    // Get Initial Status
    myPreviewLogsApi.getLogsStatus({
      namespace : vm.namespaceId,
      previewId : vm.previewId
    }).$promise.then(
      (statusRes) => {
        LogViewerStore.dispatch({
          type: LOGVIEWERSTORE_ACTIONS.SET_STATUS,
          payload: {
            status: statusRes.status,
          }
        });
        vm.setProgramMetadata(statusRes.status);
      },
      (statusErr) => {
        console.log('ERROR: ', statusErr);

        if (statusErr.statusCode === 404) {
          myAlertOnValium.show({
            type: 'danger',
            content: statusErr.data
          });
        }
      });
  }

  vm.filterSearch = () => {
    // Rerender data
    vm.renderData();
    // If the search query is blank, otherwise filter
    if (vm.searchText.length === 0){
      vm.updateSearchResultsInStore([]);
      return;
    }

    let searchResults = [];

    vm.displayData = vm.displayData.filter( data => {
      if (data.log.message.toLowerCase().indexOf(vm.searchText.toLowerCase()) !== -1){
        searchResults.push(data.log.timestamp);
        return true;
      }
      return false;
    });

    vm.updateSearchResultsInStore(searchResults);
  };

  vm.toggleStackTrace = (index) => {
    //If the currently clicked row is a stack trace itself, do nothing
    if(vm.displayData[index].stackTrace){
      return;
    }

    if( (index+1 < vm.displayData.length) && vm.displayData[index+1].stackTrace){
      vm.displayData.splice(index+1, 1);
      vm.displayData[index].selected = false;
      return;
    }
    if(vm.displayData[index].log.stackTrace){
      vm.displayData[index].selected = true;
      let stackTraceObj = {
        log: {
          timestamp: vm.displayData[index].log.timestamp,
          stackTrace: vm.displayData[index].log.stackTrace,
        },
        stackTrace: true
      };
      vm.displayData.splice(index+1, 0, stackTraceObj);
    } else {
      //otherwise, it does not have stack trace but has been selected
      vm.displayData[index].selected = !vm.displayData[index].selected;
    }
  };

  vm.collapseColumns = () => {
    if(vm.isMessageExpanded){
      vm.isMessageExpanded = !vm.isMessageExpanded;
    }
    if(collapseCount < columnsList.length){
      vm.hiddenColumns[columnsList[collapseCount++]] = true;
      if(collapseCount === columnsList.length){
        vm.isMessageExpanded = true;
      }
    } else {
      collapseCount = 0;
      for(var key in vm.hiddenColumns){
        if(vm.hiddenColumns.hasOwnProperty(key)){
          vm.hiddenColumns[key] = false;
        }
      }
    }
  };

  vm.updateScrollPositionInStore = (val) => {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION,
      payload: {
        scrollPosition: val
      }
    });
  };

  vm.updateScreenChangeInStore = (state) => {

    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.FULL_SCREEN,
      payload: {
        fullScreen: state
      }
    });
  };

  vm.updateSearchResultsInStore = (results) => {
    LogViewerStore.dispatch({
      type: LOGVIEWERSTORE_ACTIONS.SEARCH_RESULTS,
      payload: {
        searchResults: results
      }
    });
  };

  function validUrl() {
    return vm.namespaceId && vm.previewId;
  }

  function requestWithOffset() {
    if (vm.loading) { return; }

    if (!validUrl()){
       vm.loading = false;
       return;
    }

    vm.loading = true;

    if (pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }

    myPreviewLogsApi.nextLogsJsonOffset({
      'namespace' : vm.namespaceId,
      'previewId' : vm.previewId,
      'fromOffset' : vm.fromOffset,
      filter: `loglevel=${vm.selectedLogLevel}`
    }).$promise.then(
      (res) => {
        vm.errorRetrievingLogs = false;
        vm.loading = false;

        if (res.length === 0) {
          offsetScroll();
          getStatus();

          if (vm.statusType !== 0) {
            vm.endRequest = true;
          }

          return;
        }

        vm.fromOffset = res[res.length-1].offset;
        angular.forEach(res, (element, index) => {
          //Format dates properly for rendering and computing
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = moment(formattedDate).format('L H:mm:ss');
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        vm.data = vm.data.concat(res);
        vm.renderData();
        if(vm.displayData.length < vm.viewLimit){
          getStatus();
        }
      },
      (err) => {
        console.log('ERROR: ', err);
        vm.errorRetrievingLogs = true;
        vm.loading = false;
        offsetScroll();
      });
  }

  function getStatus () {
    myPreviewLogsApi.getLogsStatus({
      namespace : vm.namespaceId,
      previewId : vm.previewId
    }).$promise.then(
      (statusRes) => {
        LogViewerStore.dispatch({
          type: LOGVIEWERSTORE_ACTIONS.SET_STATUS,
          payload: {
            status: statusRes.status,
          }
        });
        vm.setProgramMetadata(statusRes.status);
        if(vm.statusType === 0){
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
  }

  function pollForNewLogs () {
    pollPromise = dataSrc.poll({
      _cdapPath: '/namespaces/' + vm.namespaceId + '/previews/' + vm.previewId + '/logs/next?format=json&max=100&fromOffset=' + vm.fromOffset + '&filter=loglevel=' + vm.selectedLogLevel,
      method: 'GET'
    },
    (res) => {
      //We have recieved more logs, append to current dataset
      vm.errorRetrievingLogs = false;

      if(res.length > 0){
        vm.fromOffset = res[res.length-1].offset;

        angular.forEach(res, (element, index) => {
          //Format dates properly for rendering and computing
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = moment(formattedDate).format('L H:mm:ss');
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        vm.data = vm.data.concat(res);
        vm.renderData();
      }

      if(vm.displayData.length >= vm.viewLimit){
        dataSrc.stopPoll(pollPromise.__pollId__);
        pollPromise = null;
      } else {
        getStatus();
      }

    }, (err) => {
      console.log('ERROR: ', err);
      vm.errorRetrievingLogs = true;
    });
  }

  vm.getDownloadUrl = (type = 'download') => {

    // Generate backend path
    let startTime = Math.floor(vm.startTimeMs/1000);
    let path = `/namespaces/${vm.namespaceId}/previews/${vm.previewId}/logs?start=${startTime}&escape=false`;
    path = encodeURIComponent(myCdapUrl.constructUrl({_cdapPath: path}));

    let url = `/downloadLogs?backendUrl=${path}&type=${type}`;

    if (type === 'download') {
      // Generate filename
      let filename = '';
      if ('undefined' !== typeof this.getDownloadFilename()) {
        filename = this.getDownloadFilename() + '-' + formatDate(new Date(this.startTimeMs), true);
      } else {
         filename = this.namespaceId + '-' + this.previewId + formatDate(new Date(this.startTimeMs), true);
      }

      url = `${url}&filename=${filename}.log`;
    }

    return url;
  };

  function startTimeRequest () {
    if(!validUrl()){
       vm.loading = false;
       return;
    }
    vm.loading = true;
    vm.data = [];
    vm.renderData();
    if(pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }

    //Scroll table to the top
    angular.element(document.getElementsByClassName('logs-table-body'))[0].scrollTop = 0;
    // binds window element to check whether scrollbar has appeared on resize event
    angular.element($window).bind('resize', checkForScrollbar);

    // myPreviewLogsApi.nextLogsJsonOffset({
    myPreviewLogsApi.nextLogsJson({
      namespace : vm.namespaceId,
      previewId : vm.previewId,
      // fromOffset: vm.fromOffset,
      filter: `loglevel=${vm.selectedLogLevel}`
    }).$promise.then(
      (res) => {
        vm.errorRetrievingLogs = false;
        vm.loading = false;
        vm.data = res;

        if(res.length === 0){
          //Update with start-time
          getStatus();
          if(vm.statusType !== 0){
            // vm.loading = false;
            vm.displayData = [];
          }
          vm.renderData();
          return;
        }

        // vm.fromOffset = res[res.length-1].offset;
        vm.displayData = [];

        angular.forEach(res, (element, index) => {
          let formattedDate = new Date(res[index].log.timestamp);
          res[index].log.timestamp = formattedDate;
          res[index].log.displayTime = moment(formattedDate).format('L H:mm:ss');
          res[index].log.stackTrace = res[index].log.stackTrace.trim();
        });

        vm.renderData();
        //Update the scroll needle to be positioned at the first element in the rendered data
        if(vm.displayData.length > 0){
          vm.updateScrollPositionInStore(vm.displayData[0].log.timestamp);
        }

        if(res.length < vm.viewLimit){
          getStatus();
        }
      },
      (err) => {
        vm.setDefault();
        vm.errorRetrievingLogs = true;
        console.log('ERROR: ', err);
      });
  }

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

  vm.toggleLogExpansion = function() {
    let len = vm.displayData.length;
    vm.toggleExpandAll = !vm.toggleExpandAll;
    for(var i = 0 ; i < len ; i++) {
      let entry = vm.displayData[i];
      if(!entry.stackTrace && entry.log.stackTrace.length > 0){
        entry.isStackTraceExpanded = vm.toggleExpandAll;

        if(i < vm.displayData.length && vm.toggleExpandAll && (i+1 === vm.displayData.length || !vm.displayData[i+1].stackTrace)){
          vm.displayData[i].selected = true;
          let stackTraceObj = {
            log: {
              timestamp: vm.displayData[i].log.timestamp,
              stackTrace: vm.displayData[i].log.stackTrace,
            },
            stackTrace: true
          };
          vm.displayData.splice(i+1, 0, stackTraceObj);
          len++;
        } else if(!vm.toggleExpandAll && !entry.stackTrace && i+1 < vm.displayData.length && vm.displayData[i+1].stackTrace){
          vm.displayData[i].selected = false;
          vm.displayData.splice(i+1, 1);
          len--;
        }
      } else {
        vm.displayData[i].selected = vm.toggleExpandAll;
      }
    }
    checkForScrollbar();
  };

  vm.includeEvent = function(event, eventType){
    if (eventType === vm.selectedLogLevel) {
      event.preventDefault();
      return;
    }

    vm.endRequest = false;

    // reset log levels
    vm.activeLogLevels = {
      'ERROR' : false,
      'WARN' : false,
      'INFO' : false,
      'DEBUG' : false,
      'TRACE' : false
    };

    switch (eventType) {
      case 'TRACE':
        vm.activeLogLevels['TRACE'] = true;
        /* falls through */
      case 'DEBUG':
        vm.activeLogLevels['DEBUG'] = true;
        /* falls through */
      case 'INFO':
        vm.activeLogLevels['INFO'] = true;
        /* falls through */
      case 'WARN':
        vm.activeLogLevels['WARN'] = true;
        /* falls through */
      case 'ERROR':
        vm.activeLogLevels['ERROR'] = true;
    }

    // Whenever we change the log level filter, the data needs
    // to start from scratch
    vm.data = [];
    vm.fromOffset = -10000 + '.' + vm.startTimeMs;

    vm.selectedLogLevel = eventType;
    startTimeRequest();
  };

  vm.renderData = () => {
    /*
      This function used to do the UI only filtering for log level.
      Not sure all the different scenarios. Since the log level filter
      now comes from backend, all the function needs to do now is set the
      displayData to the data list we get from backend
    */
    //Clean slate
    vm.displayData = vm.data;
    checkForScrollbar();
  };

  vm.highlight = (text) => {
    if(!vm.searchText || (vm.searchText && !vm.searchText.length)){
     return $sce.trustAsHtml(text);
    }

    return $sce.trustAsHtml(
      text.replace(new RegExp(vm.searchText, 'gi'),
      '<span class="highlighted-text">$&</span>'
    ));
  };

  function offsetScroll() {
    let logsTableContainer = document.getElementsByClassName('logs-table-body');

    if (logsTableContainer && Array.isArray(logsTableContainer)) {
      // Offset the scroll by a tiny margin to stop the infinite scroll to
      // fire repeatedly
      logsTableContainer[0].scrollTop = logsTableContainer[0].scrollTop - 15;
    }
  }

  vm.scrollFn = _.throttle(function() {
    if (vm.loading || vm.endRequest) { return; }
    offsetScroll();
    requestWithOffset();
  }, 500);

  vm.preventClick = function (event) {
    // disabled is not a valid attribute for <a>
    if (!vm.displayData.length || vm.loading) {
      event.preventDefault();
    }
  };

  function checkForScrollbar() {
    // has to do timeout here for Firefox
    timeout = $timeout(function(){
      let tbodyEl = angular.element(document.querySelector('.logs-table-body'))[0];
      if (tbodyEl.clientHeight < tbodyEl.scrollHeight) {
        let bodyRowEl = angular.element(document.querySelector('.logs-table-body tr'))[0];
        vm.headerRowStyle = {
          width: bodyRowEl.offsetWidth + 'px'
        };
      } else {
        vm.headerRowStyle = {};
      }
    }, 100);
  }

  // This is only for users who have their scrollbar preferences set to always show.
  // When the user resizes the browser, a scrollbar over the entire browser will appear
  // for about a second after the user stops resizing, then disappear, which will cause
  // the width of the inner elements to change. This $watch is to handle showing the
  // correct header width when that happens.
  $scope.$watch(function() {
    return angular.element(document.querySelector('.logs-table-body tr'))[0].offsetWidth;
  }, function(newValue, oldValue) {
    // different browsers have different width for the scrollbar
    if (newValue >= oldValue + 15 && newValue <= oldValue + 20) {
      vm.headerRowStyle = {
        width: newValue + 'px'
      };
    } else {
      vm.headerRowStyle = {};
    }
  });

  $scope.$on('$destroy', function() {
    if (unsub) {
      unsub();
    }
    if (timeout) {
      $timeout.cancel(timeout);
    }
    angular.element($window).unbind('resize', checkForScrollbar);
    LogViewerStore.dispatch({
      type: 'RESET'
    });
    if(pollPromise){
      dataSrc.stopPoll(pollPromise.__pollId__);
      pollPromise = null;
    }
  });
}

const link = (scope) => {
  scope.tableEl = document.getElementsByClassName('logs-table-body');
};

angular.module(PKG.name + '.commons')
  .value('THROTTLE_MILLISECONDS', 250) // throttle infinite scroll
  .directive('myLogViewerPreview', function () {
    return {
      templateUrl: 'log-viewer-preview/log-viewer-preview.html',
      controller: LogViewerPreviewController,
      controllerAs: 'LogViewerPreview',
      scope: {
        displayOptions: '=?',
        namespaceId: '@',
        previewId: '@',
        getDownloadFilename: '&',
        entityName: '@'
      },
      link: link,
      bindToController: true
    };
  });
