angular.module(PKG.name+'.feature.home')
  .config(function ($stateProvider, $urlRouterProvider, $urlMatcherFactoryProvider) {

    /**
     * Ignores trailing slash
     */
    $urlMatcherFactoryProvider.strictMode(false);

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
        templateUrl: '/assets/features/home/home.html',
        onEnter: function(MY_CONFIG, myAuth, $state, myLoadingService) {
          if (!MY_CONFIG.securityEnabled) {
            // Skip even the login view. Don't show login if security is disabled.
            myAuth.login({username:'admin'})
              .then(function() {
                myLoadingService.showLoadingIcon();
                $state.go('overview');
              });
          }
        }
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
        controller: function ($state, rNsList, mySessionStorage, myLoadingService, myAlert, $filter, EventPipe) {
          // check that $state.params.namespace is valid
          var n = rNsList.filter(function (one) {
            return one.name === $state.params.namespace;
          });

          function checkNamespace (ns) {
            var def = $filter('filter')(rNsList, { name: ns }, true);
            return def.length > 0 ? true : false;
          }

          var PREFKEY = 'feature.home.ns.latest';

          if(!n.length) {
            mySessionStorage.get(PREFKEY)
              .then(function (latest) {

                if (checkNamespace(latest)) {
                  $state.go($state.current, {namespace: latest}, {reload: true});
                  return;
                }
                //check for default
                if (checkNamespace('default')){
                  $state.go($state.current, {namespace: 'default'}, {reload: true});
                  return;
                }

                // evoke backend is down
                EventPipe.emit('backendDown');

              });
          }
          else {
            mySessionStorage.set(PREFKEY, $state.params.namespace);
          }
          myLoadingService.hideLoadingIcon();
        }
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
