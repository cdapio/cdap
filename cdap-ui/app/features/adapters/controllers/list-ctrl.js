angular.module(PKG.name + '.feature.adapters')
  .controller('AdapterListController', function($scope, mySettings, $state, $alert, $timeout, myAlert, myHelpers) {
    $scope.adapters  = [];

    mySettings.get('adapterDrafts')
      .then(function(res) {
        if (res && Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            $scope.adapters.push({
              isdraft: true,
              name: key,
              template: myHelpers.objectQuery(value, 'config', 'metadata', 'type') || myHelpers.objectQuery(value, 'template'),
              status: '-',
              description: myHelpers.objectQuery(value, 'config', 'metadata', 'description') || myHelpers.objectQuery(value, 'description')
            });
          });
        }
      });

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

    $scope.deleteDraft = function(draftName) {
      mySettings.get('adapterDrafts')
        .then(function(res) {
          if (res[draftName]) {
            delete res[draftName];
          }
          return mySettings.set('adapterDrafts', res);
        })
        .then(
          function success() {
            $alert({
              type: 'success',
              content: 'Adapter draft ' + draftName + ' delete successfully'
            });
            $state.reload();
          },
          function error() {
            $alert({
              type: 'danger',
              content: 'Adapter draft ' + draftName + ' delete failed'
            });
            $state.reload();
          });
    }

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
