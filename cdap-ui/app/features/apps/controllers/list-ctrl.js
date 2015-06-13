angular.module(PKG.name + '.feature.apps')
  .controller('AppListController', function CdapAppList($timeout, $scope, MyDataSource, myAppUploader, $alert, $state, MyOrderings) {
    this.MyOrderings = MyOrderings;
    var data = new MyDataSource($scope);

    data.request({
      _cdapNsPath: '/apps/'
    })
      .then(function(apps) {
        this.apps = apps;
      }.bind(this));
      this.onFileSelected = myAppUploader.upload;

  });
