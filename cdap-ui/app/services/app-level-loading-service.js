angular.module(PKG.name + '.services')
  .service('myLoadingService', function($q, EventPipe) {
    var deferred;
    this.showLoadingIcon = function() {
      if (deferred) {
        return deferred.promise;
      } else {
        deferred = $q.defer();
        EventPipe.emit('showLoadingIcon');
        deferred.resolve(true);
        return deferred.promise;
      }
    };

    this.hideLoadingIcon = function() {
      if (!deferred) {
        return $q.when(true);
      } else {
        EventPipe.emit('hideLoadingIcon');
        deferred.resolve(true);
        deferred = null;
      }
    };

  });
