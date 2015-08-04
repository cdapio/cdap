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
    widget = {
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
              return DashboardHelper.fetchData(widget);
            } else {
              $scope.formattedData = [];
            }
          },
          function onMetricsDiscoveryError() {
            console.error('Error on Metrics fetch');
          })
        .then(
          function onMetricsFetchSuccess(metrics) {
            if (!widget.formattedData || !widget.formattedData.columns) {
              return;
            }
            $scope.data = widget.formattedData.columns;
            $scope.formattedData = [];
            $scope.data.map(function(metric) {
              var obj = {};
              if (metric[0].indexOf('records.in') !== -1 ||
                  metric[0].indexOf('records.out') !== -1) {
                    obj.name = metric[0];
                    obj.value = metric[1];
                    $scope.formattedData.push(obj);
                  }
            });
          },
          function onMetricsFetchError() {
            console.log('Error in fetching values for metrics: ', widget.metric.names);
          });
  });
