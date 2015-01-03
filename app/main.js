console.time(PKG.name);

angular
  .module(PKG.name, [

    angular.module(PKG.name+'.features', [
      PKG.name+'.feature.home',
      PKG.name+'.feature.overview',
      PKG.name+'.feature.login',
      PKG.name+'.feature.dashboard',
      PKG.name+'.feature.cdap-app',
      PKG.name+'.feature.admin',
      PKG.name+'.feature.foo'
    ]).name,

    angular.module(PKG.name+'.commons', [

      angular.module(PKG.name+'.services', [
        PKG.name+'.config',
        'ngAnimate',
        'ngSanitize',
        // 'ngResource',
        'ngStorage',
        'ui.router',
        'cask-angular-window-manager',
        'cask-angular-theme',
        'cask-angular-focus'
      ]).name,

      angular.module(PKG.name+'.filters', [
        PKG.name+'.services',
        'cask-angular-capitalize'
      ]).name,

      'cask-angular-sortable',
      'cask-angular-confirmable',
      'cask-angular-promptable',
      'mgcrea.ngStrap.alert',
      'mgcrea.ngStrap.tooltip',
      'mgcrea.ngStrap.popover',
      'mgcrea.ngStrap.dropdown',
      'mgcrea.ngStrap.collapse',
      'mgcrea.ngStrap.button',
      'mgcrea.ngStrap.tab',
      'mgcrea.ngStrap.modal',
      'ncy-angular-breadcrumb'

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


  .config(function ($locationProvider) {
    $locationProvider.html5Mode(true);
  })

  .config(function ($alertProvider) {
    angular.extend($alertProvider.defaults, {
      animation: 'am-fade-and-scale',
      container: '#alerts > .container',
      duration: 3
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
      'cdap',   // customized theme
      'default' // bootstrap default theme
    ]);
  })


  .run(function ($rootScope, MYSOCKET_EVENT, $alert) {

    $rootScope.$on(MYSOCKET_EVENT.closed, function (angEvent, sockEvent) {
      $alert({
        title: 'Error',
        content: sockEvent.reason || 'could not connect to the server',
        type: 'danger'
      });
    });

    $rootScope.$on(MYSOCKET_EVENT.message, function (angEvent, data) {

      if(data.statusCode>399) {
        $alert({
          title: data.statusCode.toString(),
          content: data.response || 'Something went terribly wrong',
          type: 'danger'
        });
      }

      if(data.warning) {
        $alert({
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
  .controller('BodyCtrl', function ($scope, caskTheme, CASK_THEME_EVENT) {

    var activeThemeClass = caskTheme.getClassName();


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
    });



    console.timeEnd(PKG.name);
  });
