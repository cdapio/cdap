angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespaceProvider($q, MyDataSource, myNamespaceMediator, $rootScope, $state) {

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
      if ($state.params.namespaceId) {
        deferred.resolve($state.params.namespaceId);
      } else {
        this.getList()
          .then(function(list) {
            myNamespaceMediator.setCurrentNamespace(list[0]);
            $state.params.namespaceId=list[0].name;
            deferred.resolve(list[0]);
          });
      }
      return deferred.promise;
    };
});
