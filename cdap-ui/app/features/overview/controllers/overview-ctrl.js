/**
 * OverviewCtrl
 */

angular.module(PKG.name+'.feature.overview').controller('OverviewCtrl',
function ($scope, MyDataSource, $state, myLocalStorage, MY_CONFIG, Widget, MyOrderings, MyMetricsQueryHelper, myHelpers, MyChartHelpers, myStreamApi, myDatasetApi) {
  $scope.MyOrderings = MyOrderings;

  if(!$state.params.namespace) {
    // the controller for "ns" state should handle the case of
    // an empty namespace. but this nested state controller will
    // still be instantiated. avoid making useless api calls.
    return;
  }

  $scope.apps = [];

  $scope.dataList = [];
  $scope.hideWelcomeMessage = false;


  var dataSrc = new MyDataSource($scope),
      PREFKEY = 'feature.overview.welcomeIsHidden';

  myLocalStorage.get(PREFKEY)
    .then(function (v) {
      $scope.welcomeIsHidden = v;
    });

  $scope.hideWelcome = function () {
    myLocalStorage.set(PREFKEY, true);
    $scope.welcomeIsHidden = true;
  };

  $scope.isEnterprise = MY_CONFIG.isEnterprise;
  $scope.systemStatus = '#C9C9D1';
  dataSrc.poll({
    _cdapPath: '/system/services/status',
    interval: 10000
  }, function(res) {
    var serviceStatuses = Object.keys(res).map(function(value) {
      return res[value];
    });
    if (serviceStatuses.indexOf('NOT OK') > -1) {
      $scope.systemStatus = 'yellow';
    }
    if (serviceStatuses.indexOf('OK') === -1) {
      $scope.systemStatus = 'red';
    }
    if (serviceStatuses.indexOf('NOT OK') === -1) {
      $scope.systemStatus = 'green';
    }
  });

  dataSrc.request({
    _cdapNsPath: '/apps'
  })
    .then(function(res) {
      $scope.apps = res;
    });

  var params = {
    namespace: $state.params.namespace,
    scope: $scope
  };

  myDatasetApi.list(params)
    .$promise
    .then(function(res) {
      $scope.dataList = $scope.dataList.concat(res);
    });

  myStreamApi.list(params)
    .$promise
    .then(function(res) {
      if (angular.isArray(res) && res.length) {
        angular.forEach(res, function(r) {
          r.type = 'Stream';
        });

        $scope.dataList = $scope.dataList.concat(res);
      }
    });

  $scope.wdgts = [];
  // type field is overridden by what is rendered in view because we do not use widget.getPartial()
  var widgetSettings = {
    size: {
      height: 100,
      width: 280
    },
    chartMetadata: {
      showx: false,
      showy: false,
      legend: {
        show: false,
        position: 'inset'
      }
    },
    color: {
      pattern: ['red', '#f4b400']
    },
    isLive: true,
    interval: 60*1000,
    aggregate: 5
  };

  $scope.wdgts.push(
    new Widget({
      title: 'System',
      type: 'c3-line',
      settings: widgetSettings,
      metric: {
        context: 'namespace.*',
        names: ['system.services.log.error', 'system.services.log.warn'],
        startTime: 'now-3600s',
        endTime: 'now',
        resolution: '1m'
      },
      metricAlias: {
        'system.services.log.error': 'System Errors',
        'system.services.log.warn' : 'System Warnings'
      }
    })
  ,
    new Widget({
      title: 'Applications',
      type: 'c3-line',
      settings: widgetSettings,
      metric: {
        context: 'namespace.' + $state.params.namespace,
        names: ['system.app.log.error', 'system.app.log.warn'],
        startTime: 'now-3600s',
        endTime: 'now',
        resolution: '1m'
      },
      metricAlias: {
        'system.app.log.error': 'Application Errors',
        'system.app.log.warn' : 'Application Warnings'
      }
    })
  );

  // TODO: There should be a better way. Right now the 'sort-of' legend for
  // #of warnings and errors are outside the charts. Thats the reason
  // polling happens in two different places.
  angular.forEach($scope.wdgts, function(widget) {
    dataSrc.poll({
      _cdapPath: '/metrics/query',
      method: 'POST',
      body: MyMetricsQueryHelper.constructQuery(
        'qid',
        MyMetricsQueryHelper.contextToTags(widget.metric.context),
        widget.metric
      )
    }, function(res) {

      var processedData = MyChartHelpers.processData(
        res,
        'qid',
        widget.metric.names,
        widget.metric.resolution,
        widget.settings.aggregate
      );
      processedData = MyChartHelpers.c3ifyData(processedData, widget.metric, widget.metricAlias);
      widget.chartData = angular.copy(processedData);
    });
  });

});
