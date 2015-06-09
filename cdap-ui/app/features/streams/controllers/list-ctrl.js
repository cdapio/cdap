angular.module(PKG.name + '.feature.streams')
  .controller('StreamsListController', function($scope, MyDataSource) {
    var dataSrc = new MyDataSource($scope);

    dataSrc.request({
      _cdapNsPath: '/streams'
    })
      .then(function(res) {
        this.streams = res;
      }.bind(this));
  });
