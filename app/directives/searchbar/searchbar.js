/**
 * mySearchbar
 */

angular.module(PKG.name+'.commons').directive('mySearchbar',
function mySearchbarDirective ($dropdown, caskTheme) {
  return {
    restrict: 'A',
    templateUrl: 'searchbar/searchbar.html',

    link: function (scope, element, attrs) {


    }
  };
});
