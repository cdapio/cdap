angular.module(PKG.name + '.services')
  .service('myNamespaceMediator', function myNameSpaceMediator($q) {
    this.currentNamespace = null;
    this.namespaceList = [];
    var currentNsdeferred = $q.defer(),
        nsListdeferred = $q.defer();;

    this.setCurrentNamespace = function(ns) {
      this.currentNamespace = ns;
      currentNsdeferred.resolve(this.currentNamespace);
    };

    this.setNamespaceList = function (nsList) {
      this.namespaceList = nsList;
      this.setCurrentNamespace(nsList[0]);
      nsListdeferred.resolve(this.namespaceList);
    };

    this.getCurrentNamespace = function() {
      if (this.currentNamespace) {
        currentNsdeferred.resolve(this.currentNamespace);
      }
      return currentNsdeferred.promise;
    };

    this.getNamespaceList = function() {
      if (this.namespaceList.length > 0) {
        nsListdeferred.resolve(this.namespaceList);
      }
      return nsListdeferred.promise;
    };

  });
