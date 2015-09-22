/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

angular.module(PKG.name+'.feature.home')
  .config(function ($stateProvider, $urlRouterProvider, $urlMatcherFactoryProvider, MYAUTH_ROLE) {

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
        onEnter: function(MY_CONFIG, myAuth, $state, myLoadingService, $rootScope, MYAUTH_EVENT) {
          if (!MY_CONFIG.securityEnabled) {
            // Skip even the login view. Don't show login if security is disabled.
            myAuth.login({username:'admin'})
              .then(function() {
                myLoadingService.showLoadingIcon();
                $state.go('overview');
              });
          } else {
            if (myAuth.isAuthenticated()) {
              $rootScope.$broadcast(MYAUTH_EVENT.loginSuccess);
            } else {
              $state.go('login');
            }
          }
        }
      })

      .state('ns', {
        url: '/ns/:namespace',
        abstract: true,
        template: '<ui-view/>',
        data: {
          authorizedRoles: MYAUTH_ROLE.all,
          highlightTab: 'development'
        },
        resolve: {
          rNsList: function (myNamespace) {
            return myNamespace.getList();
          }
        },
        controller: 'HomeController'
      })

      .state('404', {
        templateUrl: '/assets/features/home/404.html'
      })

      ;


  });
