/**
 * mySearchbar
 */

angular.module(PKG.name+'.commons').directive('mySearchbar',
function mySearchbarDirective ($alert) {
  return {
    restrict: 'A',
    templateUrl: 'searchbar/searchbar.html',

    link: function (scope, element, attrs) {

      element.find('form').on('submit', function(e) {
        $alert({
          title: 'sorry',
          content: 'it does not work yet',
          type: 'danger'
        });
      })

    }
  };
});
