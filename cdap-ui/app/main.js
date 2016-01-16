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
      PKG.name+'.feature.home',
      PKG.name+'.feature.overview',
      PKG.name+'.feature.login',
      PKG.name+'.feature.dashboard',
      PKG.name+'.feature.apps',
      PKG.name+'.feature.data',
      PKG.name+'.feature.admin',
      PKG.name+'.feature.userprofile',
      PKG.name+'.feature.experimental',
      PKG.name+'.feature.hydrator',
      PKG.name+'.feature.explore',
      PKG.name +'.feature.search',
      PKG.name +'.feature.pins'
    ]).name,

    angular.module(PKG.name+'.commons', [

      angular.module(PKG.name+'.services', [
        PKG.name+'.config',
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
      'hc.marked'

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
          if (config.params && config.params.scope) {
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
            angular.extend(config, {
              user: $rootScope.currentUser || null,
              headers: {
                'Content-Type': 'application/json',
                // Accessing stuff from $rootScope is bad. This is done as to resolve circular dependency.
                // $http <- myAuthPromise <- myAuth <- $http <- $templateFactory <- $view <- $state
                Authorization: ($rootScope.currentUser.token ? 'Bearer ' + $rootScope.currentUser.token: null)
              }
            });
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
      duration: 3
    });
  })

  .config(function ($bootstrapTooltipProvider) {
    $bootstrapTooltipProvider.setTriggers({
      'customShow': 'customHide'
    });
  })

  .config(function ($compileProvider) {
    $compileProvider.aHrefSanitizationWhitelist(
      /^\s*(https?|ftp|mailto|tel|file|blob):/
    );
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
  /*
    FIXME: This is a one time only thing. Once all old users who migrated to 3.3 have their drafts moved from global level to
          namespace level this snippet can be removed. Ideally in 4.* we should be able to remove this.
  */
  .run(function(mySettings, EventPipe, $state, $alert, $q) {
    mySettings.get('adapterDrafts')
      .then(
        function success(res) {
          var namespacedDrafts = {
            default: {}
          };
          if (res && !res.isMigrated) {
            angular.forEach(res, function(draft, name) {
               namespacedDrafts.default[name] = draft;
            });

            namespacedDrafts.isMigrated = true;
            return mySettings.set('adapterDrafts', namespacedDrafts);
          } else {
            return $q.reject(false);
          }
        }
      )
      .then(
        function showAlert() {
          $alert({
            type: 'info',
            content: 'All current drafts can be found in Default namespace.'
          });
        }
      );
  })

  .run(function (MYSOCKET_EVENT, myAlert, EventPipe) {
    EventPipe.on(MYSOCKET_EVENT.closed, function (angEvent, data) {
      myAlert({
        title: 'Error',
        content: data.reason || 'Unable to connect to CDAP',
        type: 'danger'
      });
    });

    EventPipe.on(MYSOCKET_EVENT.message, function (data) {
      if(data.statusCode > 399 && !data.resource.suppressErrors) {
        myAlert({
          title: data.statusCode.toString(),
          content: data.response || 'Server had an issue, please try refreshing the page',
          type: 'danger'
        });
      }

      // The user doesn't need to know that the backend node
      // is unable to connect to CDAP. Error messages add no
      // more value than the pop showing that the FE is waiting
      // for system to come back up. Most of the issues are with
      // connect, other than that pass everything else to user.
      if(data.warning && data.error.syscall !== 'connect') {
        myAlert({
          content: data.warning,
          type: 'warning'
        });
      }
    });
  })

  /**
   * BodyCtrl
   * attached to the <body> tag, mostly responsible for
   *  setting the className based events from $state and caskTheme
   */
  .controller('BodyCtrl', function ($scope, $cookies, $cookieStore, caskTheme, CASK_THEME_EVENT, $rootScope, $state, $log, MYSOCKET_EVENT, MyCDAPDataSource, MY_CONFIG, MYAUTH_EVENT, EventPipe, myAuth, $window) {

    var activeThemeClass = caskTheme.getClassName();
    var dataSource = new MyCDAPDataSource($scope);
    if (MY_CONFIG.securityEnabled) {
      if (myAuth.isAuthenticated()) {
        getVersion();
      } else {
        $rootScope.$on(MYAUTH_EVENT.loginSuccess, getVersion);
      }
    } else {
      getVersion();
    }

    $scope.copyrightYear = new Date().getFullYear();

    function getVersion() {
      dataSource.request({
        _cdapPath: '/version'
      })
        .then(function(res) {
          $scope.version = res.version;
          $rootScope.cdapVersion = $scope.version;
        });
    }

    $scope.$on(CASK_THEME_EVENT.changed, function (event, newClassName) {
      if(!event.defaultPrevented) {
        $scope.bodyClass = $scope.bodyClass.replace(activeThemeClass, newClassName);
        activeThemeClass = newClassName;
      }
    });

    $scope.$on('$stateChangeSuccess', function (event, state) {
      var classes = [];
      if(state.data && state.data.bodyClass) {
        classes = [state.data.bodyClass];
      }
      else {
        var parts = state.name.split('.'),
            count = parts.length + 1;
        while (1<count--) {
          classes.push('state-' + parts.slice(0,count).join('-'));
        }
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
      $log.log('[DataSource] reconnected, reloading...');

      // https://github.com/angular-ui/ui-router/issues/582
      $state.transitionTo($state.current, $state.$current.params,
        { reload: true, inherit: true, notify: true }
      );
    });

    $rootScope.$on('$stateChangeError', function () {
      $state.go('login');
    });
    console.timeEnd(PKG.name);
  });
