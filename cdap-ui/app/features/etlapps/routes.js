angular.module(PKG.name + '.feature.etlapps')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('etlapps', {
        url: '/etlapps',
        abstract: true,
        parent: 'apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

        .state('etlapps.list', {
          url: '',
          templateUrl: '/assets/features/etlapps/templates/list.html'
        })

        .state('etlapps.list.create', {
          url: '/create',
          onEnter: function($bootstrapModal, $state) {
            $bootstrapModal.open({
              templateUrl: '/assets/features/etlapps/templates/create.html',
              size: 'lg',
              backdrop: true,
              keyboard: true,
              controller: 'ETLAppsCreateController'
            }).result.finally(function() {
              $state.go('etlapps.list');
            });
          }
        })

        .state('etlapps.detail', {
          url: '',
          template: '<h2> EtlApps detail</h2>'
        })


  });
