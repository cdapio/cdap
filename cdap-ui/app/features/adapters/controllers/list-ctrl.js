angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterListController', function($scope, mySettings, $state, $alert, $timeout, myAdapterApi, myAlert) {
    $scope.adapters  = [];

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

    myAdapterApi.list(params)
      .$promise
      .then(function(res) {
        if (!res.length) {
          return;
        }
        $scope.adapters = $scope.adapters.concat(res);
        angular.forEach($scope.adapters, function(app) {
          if (!app.isdraft)  {
            pollStatus(app);
          }
        });
      });
    mySettings.get('adapterDrafts')
      .then(function(res) {
        if (res && Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            $scope.adapters.push({
              isdraft: true,
              name: key,
              template: value.config.metadata.type,
              status: '-',
              description: value.config.metadata.description
            });
          });
        }
      });

    function pollStatus(app) {
      var statusParams = angular.extend({
        adapter: app.name
      }, params);

      myAdapterApi.pollStatus(statusParams)
        .$promise
        .then(function (res) {
          app.status = res.status;
        });
    }

    $scope.deleteAdapter = function (appName) {
      var deleteParams = angular.extend({
        adapter: appName
      }, params);

      myAdapterApi.delete(deleteParams)
        .$promise
        .then(function() {
          $alert({
            type: 'success',
            content: 'Adapter ' + appName + ' deleted successfully.'
          });
          $timeout(function() {
            $state.go($state.current, $state.params, {reload: true});
          });
        }, function(err){
          myAlert({
            title: 'Adapter Delete Failed',
            content: err
          });
        });
    };

    $scope.doAction = function(action, appName) {
      var app = $scope.adapters.filter(function(app) {
        return app.name === appName;
      });

      var actionParams = angular.extend({
        adapter: appName,
        action: action
      }, params);

      myAdapterApi.action(actionParams, {});

      if (action === 'start') {
        app[0].status = 'STARTING';
      } else {
        app[0].status = 'STOPPING';
      }
    };
  });
