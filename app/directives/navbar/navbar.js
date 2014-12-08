/**
 * myNavbar
 */

angular.module(PKG.name+'.commons').directive('myNavbar',
function myNavbarDirective ($dropdown, myAuth, caskTheme) {
  return {
    restrict: 'A',
    templateUrl: 'navbar/navbar.html',

    link: function (scope, element, attrs) {

      $dropdown(angular.element(element[0].querySelector('a.dropdown-toggle')), {
        template: 'navbar/dropdown.html',
        animation: 'am-flip-x',
        placement: 'bottom-right',
        scope: scope
      });

      scope.logout = myAuth.logout;

      scope.theme = caskTheme;

      scope.navbarLinks = [
        { sref: 'apps',       label: 'Overview'     },
        { sref: 'dashboard',  label: 'Operations'   },
        { sref: 'admin',      label: 'Admin'        }
      ];

    }
  };
});
