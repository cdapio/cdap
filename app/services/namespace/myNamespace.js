angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespace($q, MyDataSource, $rootScope, $state) {

    this.namespaceList = [];

    var data = new MyDataSource($rootScope.$new()),
        queryInProgress = null;


    this.getList = function (force) {
      if (!force && this.namespaceList.length) {
          return $q.when(this.namespaceList);
      }

      if (!queryInProgress) {

        queryInProgress = $q.defer();

        data.request({
            _cdapPath: '/namespaces',
            method: 'GET'
          },
          function(res) {

            this.namespaceList = res;

            queryInProgress.resolve(res);
            queryInProgress = null;
          }
        );

      }

      return queryInProgress.promise;
    };
});
