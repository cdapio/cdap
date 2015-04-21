angular.module(PKG.name + '.feature.etlapps')
  .config(function($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {
    $stateProvider
      .state('adapters', {
        url: '/adapters',
        abstract: true,
        parent: 'apps',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        template: '<ui-view/>'
      })

        .state('adapters.list', {
          url: '',
          templateUrl: '/assets/features/etlapps/templates/list.html',
          controller: 'EtlAppsListController'
        })

        .state('adapters.create', {
          url: '/create',
          params: {
            data: null
          },
          templateUrl: '/assets/features/etlapps/templates/create.html',
          controller: 'ETLAppsCreateController'
        })

        .state('adapters.detail', {
          url: '/:adapterid',
          template: '<h2> Adapter detail</h2>'
        })


  });
