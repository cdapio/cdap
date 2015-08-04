angular.module(PKG.name + '.feature.apps')
  .controller('AppListController', function CdapAppList($timeout, $scope, MyDataSource, myAppUploader, $alert, $state, MyOrderings, myAdapterApi) {
    this.MyOrderings = MyOrderings;
    this.apps = [];
    this.currentPage = 1;
    this.searchText = '';
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/'
    })
      .then(function(apps) {
        this.apps = this.apps.concat(apps);
      }.bind(this));
    this.onFileSelected = myAppUploader.upload;

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

  });
