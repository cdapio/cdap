angular.module(PKG.name + '.feature.admin').controller('SystemServiceLogController',
function ($scope, $state, MyDataSource, $timeout) {

    var myDataSrc = new MyDataSource($scope);

    var path = '/system/services/' + encodeURIComponent($state.params.serviceName) + '/logs/';

    $scope.loadingNext = true;
    myDataSrc.request({
      _cdapPath: path + 'next?&maxSize=50'
    })
      .then(function(response) {
        $scope.logs = response;
        $scope.loadingNext = false;
      });

    $scope.loadNextLogs = function () {
      if ($scope.loadingNext) {
        return;
      }

      $scope.loadingNext = true;
      var fromOffset = $scope.logs[$scope.logs.length-1].offset;

      myDataSrc.request({
        _cdapPath: path + 'next?&maxSize=50&fromOffset=' + fromOffset
      })
        .then(function (res) {
          $scope.logs = _.uniq($scope.logs.concat(res));
          $scope.loadingNext = false;
        });
    };

    $scope.loadPrevLogs = function () {
      if ($scope.loadingPrev) {
        return;
      }

      $scope.loadingPrev = true;
      var fromOffset = $scope.logs[0].offset;

      myDataSrc.request({
        _cdapPath: path + 'prev?maxSize=50&fromOffset=' + fromOffset
      })
        .then(function (res) {
          $scope.logs = _.uniq(res.concat($scope.logs));
          $scope.loadingPrev = false;

          $timeout(function() {
            document.getElementById(fromOffset).scrollIntoView();
          });
        });
    };

});
