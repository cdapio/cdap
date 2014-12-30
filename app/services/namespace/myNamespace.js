angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespace($q, MyDataSource, $rootScope, $state) {
    this.namespaceList = [];
    var data = new MyDataSource($rootScope.$new());
    var deferred = $q.defer(),
        queryInProgress = false;
    this.getList = function() {
      if (!queryInProgress) {
        queryInProgress = true;
        if (this.namespaceList.length === 0) {
          data.request({
              _cdapPath: '/namespaces',
              method: 'GET'
            },
            function(res) {
              this.namespaceList = res;
              queryInProgress = false;
              deferred.resolve(res);
            }
          );
        } else {
          deferred.resolve(this.namespaceList);
        }
      }
      return deferred.promise;
    };
});
