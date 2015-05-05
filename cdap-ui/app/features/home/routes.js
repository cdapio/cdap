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
        templateUrl: '/assets/features/home/home.html',
        onEnter: function(MY_CONFIG, myAuth, $state, myLoadingService) {
          if (!MY_CONFIG.securityEnabled) {
            // Skip even the login view. Don't show login if security is disabled.
            myAuth.login({username:'admin'})
              .then(function() {
                myLoadingService.showLoadingIcon()
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
          rNsList: function (myNamespace, myLoadingService) {
            return myNamespace.getList();
          }
        },
        controller: function ($state, rNsList, mySessionStorage, myLoadingService, myAlert, $filter) {
          // check that $state.params.namespace is valid
          var n = rNsList.filter(function (one) {
            return one.name === $state.params.namespace;
          });


          var PREFKEY = 'feature.home.ns.latest';

          if(!n.length) {
            mySessionStorage.get(PREFKEY)
              .then(function (latest) {

                var def = $filter('filter')(rNsList, {name: 'default'}, true);

                if (def.length < 1) {
                  def = rNsList[0];
                  myAlert({
                    title: 'Cannot find default namespace',
                    content: 'Reverting to ' + def.name + ' namespace.'
                  });
                } else {
                  def = def[0];
                }

                var d = latest || def.name;
                console.warn('invalid namespace, defaulting to', d);
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
          myLoadingService.hideLoadingIcon()
        }
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
