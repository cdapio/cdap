/**
 * Namespace dropdown + breadcrumbs
 */

angular.module(PKG.name+'.commons').directive('myBreadcrumbs',

function myBreadcrumbsDirective ($dropdown, myAuth, MY_CONFIG) {
  return {
    restrict: 'E',
    templateUrl: 'breadcrumbs/breadcrumbs.html',
    controller: 'breadcrumbsCtrl',
    link: function (scope, element) {

      var toggles = element[0].querySelectorAll('a.dropdown-toggle');

      // namespace dropdown
      $dropdown(angular.element(toggles[0]), {
        template: 'navbar/namespace.html',
        animation: 'am-flip-x',
        scope: scope
      });

      scope.logout = myAuth.logout;

      scope.securityEnabled = MY_CONFIG.securityEnabled;
    }
  };
});
