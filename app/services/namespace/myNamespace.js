angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespaceProvider($q, MyDataSource, myNamespaceMediator, $rootScope, MY_CONFIG) {

    var data = new MyDataSource($rootScope.$new());

    this.getList = function() {
      var deferred = $q.defer();
      if (myNamespaceMediator.namespaceList.length === 0) {
        data.request({
            _cdapPath: '/namespaces',
            method: 'GET'
        }, function(res) {
          myNamespaceMediator.setNamespaceList(res);
          deferred.resolve(myNamespaceMediator.getNamespaceList());
        });
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
            var defaultns = {
              name: 'default',
              displayName: 'default'
            };
            myNamespaceMediator.setCurrentNamespace(list[0] || defaultns);
            deferred.resolve(list[0] || defaultns);
          });
      } else {
        deferred.resolve(myNamespaceMediator.getCurrentNamespace());
      }
      return deferred.promise;
    };
});
