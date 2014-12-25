/**
 * myNamespaceMediator
 * a layer of indirection to avoid circular dependency in mySocket
 */
angular.module(PKG.name + '.services')
  .service('myNamespaceMediator', function myNameSpaceMediator($q, $state) {
    this.currentNamespace = null;
    this.namespaceList = [];
    var currentNsdeferred = [],
        nsListdeferred = [];

    this.setNamespaceList = function (nsList) {
      this.namespaceList = nsList;
      nsListdeferred.forEach(function(def) {
        def.resolve(this.namespaceList);
      }.bind(this))
    };

    this.getCurrentNamespace = function() {
      var deferred = $q.defer();
      deferred.resolve($state.params.namespaceId);
      return deferred.promise;
    };

    this.getNamespaceList = function() {
      var deferred = $q.defer();
      if (this.namespaceList.length > 0) {
        deferred.resolve(this.namespaceList);
      } else {
        nsListdeferred.push(deferred);
      }
      return deferred.promise;
    };

  });
