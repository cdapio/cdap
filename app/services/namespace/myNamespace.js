angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespaceProvider($q, MyDataSource, myNamespaceMediator, $rootScope) {

    var data = new MyDataSource($rootScope.$new());

    this.getList = function() {
      var deferred = $q.defer();
      if (myNamespaceMediator.namespaceList.length === 0) {
        data.request({
            _cdapPath: '/namespaces',
            method: 'GET'
          },
          function(res) {
            myNamespaceMediator.setNamespaceList(res);
            deferred.resolve(res);
          }
        );
      } else {
        deferred.resolve(myNamespaceMediator.getNamespaceList());
      }
      return deferred.promise;
    };

    this.getCurrentNamespace = function() {
      var deferred = $q.defer();
      if (!myNamespaceMediator.currentNamespace) {
        this.getList()
          .then(function(list) {
            myNamespaceMediator.setCurrentNamespace(list[0]);
            deferred.resolve(list[0]);
          });
      } else {
        deferred.resolve(myNamespaceMediator.currentNamespace);
      }
      return deferred.promise;
    };
});
