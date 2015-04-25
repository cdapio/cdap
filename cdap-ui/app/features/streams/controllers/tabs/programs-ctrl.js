angular.module(PKG.name + '.feature.streams')
  .controller('StreamsDetailProgramsController', function($scope, $state, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/streams/' + $state.params.streamId + '/programs'
    }).then(function(res) {

      $scope.programs = res;

    });

  });