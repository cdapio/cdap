angular.module(PKG.name + '.feature.overview')
  .controller('AppsSectionCtrl', function(myAppUploader, myStreamApi, myDatasetApi, MyDataSource, MyOrderings, $scope, $state) {
    var dataSrc = new MyDataSource($scope);
    this.MyOrderings = MyOrderings;
    this.apps = [];

    this.dataList = [];
    dataSrc.request({
      _cdapNsPath: '/apps'
    })
      .then(function(res) {
        this.apps = res;
      }.bind(this));

    var params = {
      namespace: $state.params.namespace,
      scope: $scope
    };

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
