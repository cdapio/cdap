/**
 * myNavbar
 */

angular.module(PKG.name+'.commons').directive('myNavbar',
function myNavbarDirective ($dropdown, $alert, myAuth, caskTheme, MY_CONFIG) {
  return {
    restrict: 'A',
    templateUrl: 'navbar/navbar.html',

    link: function (scope, element, attrs) {

      scope.currentNamespace = "Namespace 1";

      var toggles = element[0].querySelectorAll('a.dropdown-toggle');

      // namespace dropdown
      $dropdown(angular.element(toggles[0]), {
        template: 'navbar/namespace.html',
        animation: 'am-flip-x'
      });

      // right dropdown
      $dropdown(angular.element(toggles[1]), {
        template: 'navbar/dropdown.html',
        animation: 'am-flip-x',
        placement: 'bottom-right',
        scope: scope
      });

      scope.logout = myAuth.logout;
      scope.theme = caskTheme;
      scope.securityEnabled = MY_CONFIG.securityEnabled;

      scope.navbarLinks = [
        { sref: 'home',       label: 'Development'     },
        { sref: 'dashboard',  label: 'Operations'   },
        { sref: 'admin',      label: 'Management'        }
      ];


      scope.doSearch = function () {
        $alert({
          title: 'Sorry!',
          content: 'Search is not yet implemented.',
          type: 'danger'
        });
      };
    }
  };
});
