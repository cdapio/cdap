angular.module(PKG.name + '.feature.etlapps')
  .controller('EtlAppsListController', function($scope, MyDataSource, mySettings, $state, $alert) {
    var dataSrc = new MyDataSource($scope);
    $scope.etlapps  = [];
    dataSrc.request({
      _cdapNsPath: '/adapters'
    })
      .then(function(res) {
        if (!res.length) {
          return;
        }
        $scope.etlapps = $scope.etlapps.concat(res);
        angular.forEach($scope.etlapps, function(app) {
          if (!app.isdraft)  {
            pollStatus(app);
          }
          app.template = (app.instances? 'etlRealtime': 'etlBatch');
        });
      });
    mySettings.get('etldrafts')
      .then(function(res) {
        if (Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            $scope.etlapps.push({
              isdraft: true,
              name: key,
              template: value.config.metadata.type,
              status: '-',
              description: 'Something something dark.Something Something something dark'
            });
          });
        }
      });
    $scope.dragdrop = {
      dragStart: function (drag) {
        console.log('dragStart', drag.source, drag.dest);
      },
      dragEnd: function (drag) {
        console.log('dragEnd', drag.source, drag.dest);
      }
    };

    function pollStatus(app) {
      dataSrc.poll({
        _cdapNsPath: '/adapters/' + app.name + '/status'
      }, function(res) {
        app.status = res;
      });
    }

    $scope.deleteAdapter = function (appName) {
      dataSrc.request({
        _cdapNsPath: '/adapters/' + appName,
        method: 'DELETE'
      })
        .then(function(res) {
          $alert({
            type: 'success',
            content: 'Adapter ' + appName + ' deleted successfully.'
          });
          $state.go($state.current, $state.params, {reload: true});
        }, function(err){
          console.info("Adapter Delete Failed", err);
        });
    };

    $scope.doAction = function(action, appName) {
      var app = $scope.etlapps.filter(function(app) {
        return app.name === appName;
      });

      dataSrc.request({
        _cdapNsPath: '/adapters/' + appName + '/' + action,
        method: 'POST'
      });
      if (action === 'start') {
        app[0].status = 'STARTING';
      } else {
        app[0].status = 'STOPPING';
      }
    };
  });
