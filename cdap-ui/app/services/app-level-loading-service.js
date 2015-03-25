angular.module(PKG.name + '.services')
  .service('myLoadingService', function($q, $rootScope) {
    var deferred;
    this.showLoadingIcon = function() {
      if (deferred) {
        return deferred.promise;
      } else {
        deferred = $q.defer();
        $rootScope.$broadcast('showLoadingIcon');
        deferred.resolve(true);
        return deferred.promise;
      }
    };

    this.hideLoadingIcon = function() {
      if (!deferred) {
        return $q.when(true);
      } else {
        $rootScope.$broadcast('hideLoadingIcon');
        deferred.resolve(true);
        deferred = null;
      }
    };

  });
