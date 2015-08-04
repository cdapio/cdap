angular.module(PKG.name + '.feature.overview')
  .controller('AppsSectionCtrl', function(myAppUploader, myStreamApi, myDatasetApi, MyDataSource, MyOrderings, $scope, $state, myAdapterApi) {
    var dataSrc = new MyDataSource($scope);
    this.MyOrderings = MyOrderings;
    this.apps = [];

    this.dataList = [];
    dataSrc.request({
      _cdapNsPath: '/apps'
    })
      .then(function(res) {
        this.apps = this.apps.concat(res);
      }.bind(this));

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
        res.forEach(function(adapter) {
          adapter.type = 'adapter';
        });
        this.apps = this.apps.concat(res);
      }.bind(this));

    myDatasetApi.list(params)
      .$promise
      .then(function(res) {
        this.dataList = this.dataList.concat(res);
      }.bind(this));

    myStreamApi.list(params)
      .$promise
      .then(function(res) {
        if (angular.isArray(res) && res.length) {
          angular.forEach(res, function(r) {
            r.type = 'Stream';
          });

          this.dataList = this.dataList.concat(res);
        }
      }.bind(this));
    this.onFileSelected = myAppUploader.upload;
  });
