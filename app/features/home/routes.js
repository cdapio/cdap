angular.module(PKG.name+'.feature.home')
  .config(function ($stateProvider, $urlRouterProvider, MYAUTH_ROLE) {

    /**
     * Redirects and Otherwise
     */
    $urlRouterProvider
      .otherwise(function($injector, $location){
        $injector.get('$state').go($location.path() ? '404' : 'home');
      });


    /**
     * State Configurations
     */
    $stateProvider

      .state('home', {
        url: '/',
        templateUrl: '/assets/features/home/home.html'
      })

      .state('ns', {
        url: '/ns/:namespace',
        abstract: true,
        template: '<ui-view/>',
        resolve: {
          rNsList: function (myNamespace) {
            return myNamespace.getList();
          }
        },
        controller: function ($state, $timeout, rNsList) {
          // check that $state.params.namespace is valid
          var n = rNsList.filter(function (one) {
            return one.id === $state.params.namespace;
          });

          if(!n.length) {
            var d = rNsList[0].id;
            console.warn('invalid namespace, defaulting to ', d);
            $timeout(function () {
              $state.go(
                $state.current,
                { namespace: d },
                {reload: true}
              );
            });

          }
        }
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
