angular.module(PKG.name + '.feature.adapters')
  .controller('AdaptersListController', function(myAdapterApi, $stateParams) {
    this.adaptersList = [];
    this.currentPage = 1;
    this.searchText = '';
    myAdapterApi.list({
      namespace: $stateParams.namespace
    })
      .$promise
      .then(function success(res) {
        this.adaptersList = res;
      }.bind(this));
  });
