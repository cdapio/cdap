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

    this.setCurrentNamespace = function(ns) {
      $state.params.namespaceId = ns;
      currentNsdeferred.forEach(function(def) {
        def.resolve({name: $state.params.namespaceId});
      });
    };

    this.setNamespaceList = function (nsList) {
      this.namespaceList = nsList;
      this.setCurrentNamespace(nsList[0].name);
      nsListdeferred.forEach(function(def) {
        def.resolve(this.namespaceList);
      })
    };

    this.getCurrentNamespace = function() {
      var deferred = $q.defer();
      if ($state.params.namespaceId) {
        deferred.resolve({name:$state.params.namespaceId});
      } else {
        currentNsdeferred.push(deferred);
      }
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
