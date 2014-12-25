angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespaceProvider($q, MyDataSource, myNamespaceMediator, $rootScope, $state) {

    var data = new MyDataSource($rootScope.$new());
    var deferred = $q.defer(),
        queryInProgress = false;
    this.getList = function() {
      if (!queryInProgress) {
        queryInProgress = true;
        if (myNamespaceMediator.namespaceList.length === 0) {
          data.request({
              _cdapPath: '/namespaces',
              method: 'GET'
            },
            function(res) {
              myNamespaceMediator.setNamespaceList(res);
              queryInProgress = false;
              deferred.resolve(res);
            }
          );
        } else {
          deferred.resolve(myNamespaceMediator.getNamespaceList());
        }
      }
      return deferred.promise;
    };
});
