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
        controller: function ($state, rNsList, mySessionStorage) {
          // check that $state.params.namespace is valid
          var n = rNsList.filter(function (one) {
            return one.name === $state.params.namespace;
          });


          var PREFKEY = 'feature.home.ns.latest';

          if(!n.length) {
            mySessionStorage.get(PREFKEY)
              .then(function (latest) {
                var d = latest || rNsList[0].name;
                console.warn('invalid namespace, defaulting to ', d);
                $state.go(
                  $state.current,
                  { namespace: d },
                  { reload: true }
                );
              });
          }
          else {
            mySessionStorage.set(PREFKEY, $state.params.namespace);
          }
        }
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
