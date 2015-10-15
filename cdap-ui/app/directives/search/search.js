angular.module(PKG.name + '.commons')
  .directive('mySearch', function() {
    return {
      controller: 'MySearchCtrl',
      controllerAs: 'MySearchCtrl',
      templateUrl: 'search/search.html'
    };
  });
