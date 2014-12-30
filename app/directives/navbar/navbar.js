/**
 * myNavbar
 */

angular.module(PKG.name+'.commons').directive('myNavbar',
function myNavbarDirective ($rootScope, MYAUTH_EVENT, $state, $dropdown, $alert, myAuth, caskTheme, MY_CONFIG, myNamespace) {
  return {
    restrict: 'A',
    templateUrl: 'navbar/navbar.html',

    link: function (scope, element, attrs) {

      var toggles = element[0].querySelectorAll('a.dropdown-toggle');

      $rootScope.$on (MYAUTH_EVENT.loginSuccess, function (event) {
        myNamespace.getList().then(function(list) {
          scope.namespaces = list;
          if (!$state.includes('ns.overview')) {
            scope.currentNamespace = list[0].name;
          }
        });
      });


      // namespace dropdown
      $dropdown(angular.element(toggles[0]), {
        template: 'navbar/namespace.html',
        animation: 'am-flip-x',
        scope: scope
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
        {
          sref: 'home',
          label: 'Development'
        },
        {
          sref: 'dashboard',
          label: 'Operations'
        },
        {
          sref: 'admin.overview',
          label: 'Management',
          parent: 'admin'
        }
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
