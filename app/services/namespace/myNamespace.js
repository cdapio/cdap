angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespaceProvider($q, MyDataSource, myNamespaceMediator, $rootScope) {
    var scope = $rootScope;
    this.getList = function() {
      var deferred = $q.defer();
      if (myNamespaceMediator.namespaceList.length === 0) {
        var data = new MyDataSource(scope);

        data.fetch({
          config: {
            isAbsoluteUrl: true,
            path: '/namespaces/',
            method: 'GET'
          }
        }, function(res) {
          myNamespaceMediator.setNamespaceList(res);
          deferred.resolve(res);
        });
        return deferred.promise;
      } else {
        deferred.resolve(myNamespaceMediator.getNamespaceList());
      }
      return deferred;
    };
    this.getCurrentNamespace = function(scope) {
      var deferred = $q.defer();
      if (!myNamespaceMediator.currentNamespace) {
        this.getList(scope)
          .then(function(list) {
            myNamespaceMediator.setCurrentNamespace(list[0] || 'default');
            deferred.resolve(list[0] || 'default');
          });
      } else {
        deferred.resolve(myNamespaceMediator.getCurrentNamespace());
      }
      return deferred.promise;
    };
});
