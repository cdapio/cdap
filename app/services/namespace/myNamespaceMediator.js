angular.module(PKG.name + '.services')
  .service('myNamespaceMediator', function myNameSpaceMediator($q) {
    this.currentNamespace = null;
    this.namespaceList = [];
    this.defferedList = [];
    this.deferredNsList = [];
    this.subscribers = [];

    this.setCurrentNamespace = function(ns) {
      this.currentNamespace = ns;
      angular.forEach(this.defferedList, function(deferred) {
        deferred.resolve(this.currentNamespace);
      }.bind(this));
      this.defferedList = [];
    };

    this.setNamespaceList = function (nsList) {
      this.namespaceList = nsList;
      angular.forEach(this.deferredNsList, function(deferred) {
        deferred.resolve(this.namespaceList);
      });
      this.deferredNsList = [];
      this.setCurrentNamespace(nsList[0]);
    };

    this.getCurrentNamespace = function() {
      var deferred = $q.defer();
      if (this.currentNamespace) {
        deferred.resolve(this.currentNamespace);
      } else {
        this.defferedList.push(deferred);
      }
      return deferred.promise;
    };

    this.getNamespaceList = function() {
      var deferred = $q.defer();
      if (this.namespaceList.length > 0) {
        deferred.resolve(this.namespaceList);
      } else {
        this.deferredNsList.push(deferred);
      }
      return deferred.promise;
    };

  });
