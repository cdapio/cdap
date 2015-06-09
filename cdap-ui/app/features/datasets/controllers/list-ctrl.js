angular.module(PKG.name + '.feature.datasets')
  .controller('DatasetsListController', function($scope, MyDataSource) {
    var datasrc = new MyDataSource($scope);
    this.datasets = [];
    datasrc.request({
      _cdapNsPath: '/data/datasets'
    })
      .then(function(datasets) {
        this.datasets = datasets;
      }.bind(this));
  });
