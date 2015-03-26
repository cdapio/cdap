angular.module(PKG.name + '.services')
  .service('myLoadingService', function($q, $rootScope) {
    var deferred;
    this.showLoadingIcon = function() {
      if (deferred) {
        return deferred.promise;
      } else {
        deferred = $q.defer();
        // If we are going to use a lot of event system
        // then we need a service thats handles events
        // instead of broadcasting it on $rootScope and
        // making all scopes to inherit it
        $rootScope.$broadcast('showLoadingIcon');
        deferred.resolve(true);
        return deferred.promise;
      }
    };

    this.hideLoadingIcon = function() {
      if (!deferred) {
        return $q.when(true);
      } else {
        // Same applies here as mentioned in the above comment.
        $rootScope.$broadcast('hideLoadingIcon');
        deferred.resolve(true);
        deferred = null;
      }
    };

  });
