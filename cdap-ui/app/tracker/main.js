

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

console.time(PKG.name);

angular
  .module(PKG.name, [

    angular.module(PKG.name+'.features', [
      PKG.name+'.feature.tracker',
      PKG.name+'.feature.userprofile'
    ]).name,

    angular.module(PKG.name+'.commons', [

      angular.module(PKG.name+'.services', [
        'ngAnimate',
        'ngSanitize',
        'ngResource',
        'ngStorage',
        'ui.router',
        'cask-angular-window-manager',
        'cask-angular-theme',
        'cask-angular-focus',
        'ngCookies'
      ]).name,

      angular.module(PKG.name+'.filters', [
        PKG.name+'.services',
        'cask-angular-capitalize'
      ]).name,

      'cask-angular-sortable',
      'cask-angular-confirmable',
      'cask-angular-promptable',
      'cask-angular-json-edit',
      'cask-angular-eventpipe',
      'cask-angular-observable-promise',
      'cask-angular-socket-datasource',
      'cask-angular-dispatcher',

      'mgcrea.ngStrap.datepicker',
      'mgcrea.ngStrap.timepicker',

      'mgcrea.ngStrap.alert',

      'mgcrea.ngStrap.popover',
      'mgcrea.ngStrap.dropdown',
      'mgcrea.ngStrap.typeahead',
      'mgcrea.ngStrap.select',
      'mgcrea.ngStrap.collapse',

      // 'mgcrea.ngStrap.modal',
      'ui.bootstrap.modal',
      'ui.bootstrap',

      'mgcrea.ngStrap.modal',

      'ncy-angular-breadcrumb',
      'angularMoment',
      'ui.ace',
      'gridster',
      'angular-cron-jobs',
      'angularjs-dropdown-multiselect',
      'hc.marked',
      'ngFileSaver',
      'infinite-scroll',
      'react'
    ]).name,

    'angular-loading-bar'
  ])

  .run(function ($rootScope, $state, $stateParams) {
    // It's very handy to add references to $state and $stateParams to the $rootScope
    // so that you can access them from any scope within your applications.For example,
    // <li ng-class="{ active: $state.includes('contacts.list') }"> will set the <li>
    // to active whenever 'contacts.list' or one of its decendents is active.
    $rootScope.$state = $state;
    $rootScope.$stateParams = $stateParams;

    // for debugging... or to trigger easter eggs?
    window.$go = $state.go;
  })
  .run(function($rootScope, MY_CONFIG, myAuth, MYAUTH_EVENT) {
    $rootScope.$on('$stateChangeStart', function () {
      if (MY_CONFIG.securityEnabled) {
        if (!myAuth.isAuthenticated()) {
          $rootScope.$broadcast(MYAUTH_EVENT.logoutSuccess);
        }
      }
    });
  })
  .run(function($rootScope, myHelpers, MYAUTH_EVENT) {
    $rootScope.$on(MYAUTH_EVENT.logoutSuccess, function() {
      window.location.href = myHelpers.getAbsUIUrl({
        uiApp: 'login',
        redirectUrl: location.href,
        clientId: 'hydrator'
      });
    });
  })
  .run(function(myNamespace) {
    myNamespace.getList();
  })

  .config(function (MyDataSourceProvider) {
    MyDataSourceProvider.defaultInterval = 5;
  })

  .config(function ($locationProvider) {
    $locationProvider.html5Mode(true);
  })

  .run(function ($rootScope) {
    $rootScope.defaultPollInterval = 10000;
  })

  .config(function($provide) {

    $provide.decorator('$http', function($delegate, MyCDAPDataSource) {


      function newHttp(config) {
        var promise,
            myDataSrc;
        if (config.options) {
          // Can/Should make use of my<whatever>Api service in another service.
          // So in that case the service will not have a scope. Hence the check
          if (config.params && config.params.scope && angular.isObject(config.params.scope)) {
            myDataSrc = MyCDAPDataSource(config.params.scope);
            delete config.params.scope;
          } else {
            myDataSrc = MyCDAPDataSource();
          }
          // We can use MyCDAPDataSource directly or through $resource'y way.
          // If we use $resource'y way then we need to make some changes to
          // the data we get for $resource.
          config.$isResource = true;
          switch(config.options.type) {
            case 'POLL':
              promise = myDataSrc.poll(config);
              break;
            case 'REQUEST':
              promise = myDataSrc.request(config);
              break;
            case 'POLL-STOP':
              promise = myDataSrc.stopPoll(config);
              break;
          }
          return promise;
        } else {
          return $delegate(config);
        }
      }

      newHttp.get = $delegate.get;
      newHttp.delete = $delegate.delete;
      newHttp.save = $delegate.save;
      newHttp.query = $delegate.query;
      newHttp.remove = $delegate.remove;
      newHttp.post = $delegate.post;
      newHttp.put = $delegate.put;
      return newHttp;
    });
  })

  .config(function($httpProvider) {
    $httpProvider.interceptors.push(function($rootScope, myHelpers) {
      return {
        'request': function(config) {
          if (
              $rootScope.currentUser && !myHelpers.objectQuery(config, 'data', 'profile_view')
             ) {

            var extendConfig = {
              user: $rootScope.currentUser || null,
              headers: {
                'Content-Type': 'application/json',
              }
            };

            // This check is added because of HdInsight gateway security.
            // If we set Authorization to null, it strips off their Auth token
            if ($rootScope.currentUser.token) {
              // Accessing stuff from $rootScope is bad. This is done as to resolve circular dependency.
              // $http <- myAuthPromise <- myAuth <- $http <- $templateFactory <- $view <- $state
              extendConfig.headers.Authorization = 'Bearer ' + $rootScope.currentUser.token;
            }

            angular.extend(config, extendConfig);
          }
          return config;
        }
      };
    });
  })

  .config(function ($alertProvider) {
    angular.extend($alertProvider.defaults, {
      animation: 'am-fade-and-scale',
      container: '#alerts',
      duration: false
    });
  })

  .config(function ($uibTooltipProvider) {
    $uibTooltipProvider.setTriggers({
      'customShow': 'customHide'
    });
  })

  .config(function ($compileProvider) {
    $compileProvider.aHrefSanitizationWhitelist(
      /^\s*(https?|ftp|mailto|tel|file|blob):/
    );

    /* !! DISABLE DEBUG INFO !! */
  })

  .config(function (cfpLoadingBarProvider) {
    cfpLoadingBarProvider.includeSpinner = false;
  })

  .config(function (caskThemeProvider) {
    caskThemeProvider.setThemes([
      'cdap'  // customized theme
    ]);
  })

  .config(['markedProvider', function (markedProvider) {
    markedProvider.setOptions({
      gfm: true,
      tables: true
    });
  }])

  /**
   * BodyCtrl
   * attached to the <body> tag, mostly responsible for
   *  setting the className based events from $state and caskTheme
   */
  .controller('BodyCtrl', function ($scope, $cookies, $cookieStore, caskTheme, CASK_THEME_EVENT, $rootScope, $state, $log, MYSOCKET_EVENT, MyCDAPDataSource, MY_CONFIG, MYAUTH_EVENT, EventPipe, myAuth, $window, myAlertOnValium) {

    var activeThemeClass = caskTheme.getClassName();
    var dataSource = new MyCDAPDataSource($scope);
    getVersion();

    $scope.copyrightYear = new Date().getFullYear();

    function getVersion() {
      dataSource.request({
        _cdapPath: '/version'
      })
        .then(function(res) {
          $scope.version = res.version;
          $rootScope.cdapVersion = $scope.version;

          window.CaskCommon.VersionStore.dispatch({
            type: window.CaskCommon.VersionActions.updateVersion,
            payload: {
              version: res.version
            }
          });
        });
    }

    $scope.$on(CASK_THEME_EVENT.changed, function (event, newClassName) {
      if(!event.defaultPrevented) {
        $scope.bodyClass = $scope.bodyClass.replace(activeThemeClass, newClassName);
        activeThemeClass = newClassName;
      }
    });

    $scope.$on('$stateChangeSuccess', function (event, toState, toParams, fromState) {
      var classes = [];
      if(toState.data && toState.data.bodyClass) {
        classes = [toState.data.bodyClass];
      }
      else {
        var parts = toState.name.split('.'),
            count = parts.length + 1;
        while (1<count--) {
          classes.push('state-' + parts.slice(0,count).join('-'));
        }
      }
      if(toState.name !== fromState.name && myAlertOnValium.isAnAlertOpened()) {
        myAlertOnValium.destroy();
      }
      classes.push(activeThemeClass);

      $scope.bodyClass = classes.join(' ');


      /**
       *  This is to make sure that the sroll position goes back to the top when user
       *  change state. UI Router has this function ($anchorScroll), but for some
       *  reason it is not working.
       **/
      $window.scrollTo(0, 0);
    });

    EventPipe.on(MYSOCKET_EVENT.reconnected, function () {
      $log.log('[DataSource] reconnected, reloading to home');
      /*
        TL;DR: We need to reload services to check if backend is up and running.
        LONGER-VERSION : When node server goes down and then later comes up we used to use ui-router's transitionTo api.
        This, however reloads the controllers but does not reload the services. As a result our poll for
        backend services got dropped in the previous node server and the new node server is not polling for
        backend service (system services & backend heartbeat apis). So we need to force reload the entire app
        to reload services. Ideally we should have one controller that loaded everything so that we can control
        the polling in services but unfortunately we are polling for stuff in too many places - StatusFactory, ServiceStatusFactory.
      */
      $window.location.reload();
    });

    // $rootScope.$on('$stateChangeError', function () {
    //   $state.go('login');
    // });
    console.timeEnd(PKG.name);
  });
