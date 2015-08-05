angular.module(PKG.name + '.feature.adapters')
  // TODO: We should use rAdapterDetail here since this data is already resolved at adapter.detail state
  .controller('AdapterRunDetailStatusController', function($scope, $state, myAdapterApi, CanvasFactory, MyPlumbService, DashboardHelper, MyDataSource, MyMetricsQueryHelper) {

    var datasrc = new MyDataSource($scope);

    $scope.nodes = [];
    var params = {
      namespace: $state.params.namespace,
      adapter: $state.params.adapterId,
      scope: $scope
    };

    var template;

    $scope.cloneAdapter = function() {
      if ($scope.config) {
        $state.go('adapters.create', {
          data: $scope.config,
          type: $scope.config.template
        });
      }
    };

    myAdapterApi.get(params)
      .$promise
      .then(function(res) {
        $scope.config = {
          name: $state.params.adapterId,
          template: res.template,
          description: res.description,
          config: {
            source: res.config.source,
            sink: res.config.sink,
            transforms: res.config.transforms,
            instances: res.instance,
            schedule: res.config.schedule
          }
        };

        MyPlumbService.metadata.name = res.name;
        MyPlumbService.metadata.description = res.description;
        MyPlumbService.metadata.template.type = res.template;
        if (res.template === 'ETLBatch') {
          MyPlumbService.metadata.template.schedule = res.config.schedule;
        } else if (res.template === 'ETLRealtime') {
          MyPlumbService.metadata.template.instances = res.config.instances;
        }

        $scope.source = res.config.source;
        $scope.sink = res.config.sink;
        $scope.transforms = res.config.transforms;
        $scope.nodes = CanvasFactory.getNodes(res.config);
        $scope.nodes.forEach(function(node) {
          MyPlumbService.addNodes(node, node.type);
        });

        MyPlumbService.connections = CanvasFactory.getConnectionsBasedOnNodes($scope.nodes);


      });

    var context = 'namespace.' + $state.params.namespace +
                  '.adapter.' + $state.params.adapterId;
    var tagQueryParams = MyMetricsQueryHelper
                          .tagsToParams(
                            MyMetricsQueryHelper.contextToTags(context)
                          );
    var widget = {
      metric: {
        context: context,
        names: []
      },
      settings: {
        aggregate: 61
      },
      metricAlias: {

      }
    };

    datasrc.request(
      {
        method: 'POST',
        _cdapPath: '/metrics/search?target=metric&' + tagQueryParams
      })
        .then(
          function onMetricsDiscoverySuccess(res) {
            widget.metric.names = res;
            if (res.length > 0) {
              pollForMetricsData(widget);
            } else {
              $scope.formattedData = [];
            }
          },
          function onMetricsDiscoveryError() {
            console.error('Error on Metrics fetch');
          }
        );
    function pollForMetricsData(widget) {
      DashboardHelper.pollData(widget)
        .then(
          function onMetricsFetchSuccess(metrics) {
            if (!widget.formattedData || !widget.formattedData.columns) {
              return;
            }
            $scope.formattedData = {};
            $scope.isMetrics = false;
            $scope.formattedData = {};
            widget.formattedData.columns
                  .map(extractMetricsFromData.bind(null, $scope.formattedData));
            $scope.isMetrics = Object.keys($scope.formattedData).length > 0 ? true: false;
            $scope.formattedData = Object.keys($scope.formattedData)
              .map(function(key) {
                return $scope.formattedData[key];
              });
            $scope.formattedData = orderMetricsForDisplay($scope.formattedData);
          },
          function onMetricsFetchError() {
            console.log('Error in fetching values for metrics: ', widget.metric.names);
          }
        );
    }

    function orderMetricsForDisplay(data) {
      var source = {};
      var transforms = [];
      var sink = {};
      var returnArray = [];
      source = data.filter(function(metric) {
        return metric.type === 'source';
      });
      source = source[0];
      sink = data.filter(function(metric) {
        return metric.type === 'sink';
      });
      sink = sink[0];
      transform = data.filter(function(metric) {
        return metric.type === 'transform';
      });
      returnArray = returnArray.concat(transform);
      if (source) {returnArray.unshift(source);}
      if (sink) {returnArray.push(sink);}
      return returnArray;
    }


    function extractMetricsFromData(obj, metric) {
      var matches = ['records.in', 'records.out'];
      var match = matches.filter(function(m) {
        return metric[0].indexOf(m);
      });
      if (match.length > 0) {
        var metricNameArray = metric[0].split('.');
        var metricKey = metric[0].substring(0, metric[0].lastIndexOf('.'));
        var inOut = metric[0].substring(
          metric[0].lastIndexOf('.') + 1,
          metric[0].length
        );
        obj[metricKey] =obj[metricKey] || {};
        if (metricNameArray.length === 5) {
          // No stage
          obj[metricKey].type = metricNameArray[1];
          obj[metricKey].stage = 0;
          obj[metricKey].node = metricNameArray[2];
        } else if (metricNameArray.length === 6) {
          obj[metricKey].type = metricNameArray[1];
          obj[metricKey].stage = metricNameArray[3];
          obj[metricKey].node = metricNameArray[2];
        }
        obj[metricKey].value = obj[metricKey].value || {};
        obj[metricKey].value[inOut] = metric[1];
      }
    }
  });
