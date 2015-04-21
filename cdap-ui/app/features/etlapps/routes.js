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
          templateUrl: '/assets/features/etlapps/templates/list.html',
          controller: 'EtlAppsListController'
        })

        .state('etlapps.create', {
          url: '/create',
          params: {
            data: null
          },
          templateUrl: '/assets/features/etlapps/templates/create.html',
          controller: 'ETLAppsCreateController'
        })

        .state('etlapps.detail', {
          url: '/:etlappid',
          template: '<h2> EtlApps detail</h2>'
        })


  });
