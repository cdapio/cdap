angular.module(PKG.name + '.commons')
  .controller('MySearchCtrl', function($state) {
    this.searchTxt = '';
    this.onSearch = function(event) {
      if (event.keyCode === 13) {
        $state.go('search');
      }
    };
  });
