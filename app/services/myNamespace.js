angular.module(PKG.name + '.services')
  .service('myNamespace', function myNamespace($q, MyDataSource, $state) {

    this.namespaceList = [];

    var data = new MyDataSource(),
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
          (function(res) {
            this.namespaceList = res;
            queryInProgress.resolve(res);
            queryInProgress = null;
          }).bind(this)
        );

      }

      return queryInProgress.promise;
    };
});
