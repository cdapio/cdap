// adapted from https://github.com/danmasta/ngResize/blob/703fe02301a0736d8d8892830ebb9b443a990d99/ngresize.js

/**
 * @license ngResize.js v1.1.0
 * (c) 2014 Daniel Smith http://www.danmasta.com
 * License: MIT
 */

/**
 * @name ngResize
 * @description
 *
 * # ngResize
 *
 * ngResize module provides an angular directive, provider,
 * and service for managing resize events in applications
 *
 * expressions are run when the window resizes and debounced
 * event object is available as $event.
 */

// define ngResize module
var ngResize = angular.module('ngResize', []);

ngResize.constant('MYRESIZE_EVENT', {
  name: 'myresize-throttled'
});

/*
* ngResize Provider and Service
*
* provider allows to set throttle in config
* and wether or not to bind on service initialization
*
* $broadcasts 'resize' event from $rootScope
* which gets inherited by all child scopes
*/
ngResize.provider('myResizeManager', ['MYRESIZE_EVENT', function (MYRESIZE_EVENT){

  // store throttle time
  this.throttle = 500;

  // by default bind window resize event when service
  // is initialize/ injected for first time
  this.initBind = 1;

  // service object
  this.$get = ['$rootScope', '$window', '$interval',  function ($rootScope, $window, $interval){

    var _this = this,
        bound = false,
        resized = false,
        timer = null;

    // trigger a resize event on provided $scope or $rootScope
    function trigger (scope) {
      (scope || $rootScope).$broadcast(MYRESIZE_EVENT.name, {
        width: $window.innerWidth,
        height: $window.innerHeight
      });
    }

    function bind () {
      if(!bound){
        var w = angular.element($window);
        w.on('resize', function(event) {
          if (!resized) {
            timer = $interval(function() {
              if (resized) {
                resized = false;
                $interval.cancel(timer);
                trigger();
              }
            }, _this.throttle);
          }
          resized = true;
        });
        bound = true;
        // w.triggerHandler('resize');
      }
    }

    // unbind window scroll event
    function unbind () {
      if(bound){
        var w = angular.element($window);
        w.off('resize');
        bound = false;
      }
    }

    // by default bind resize event when service is created
    if(_this.initBind){
      bind();
    }

    // return api
    return {
      trigger: trigger,
      eventName: MYRESIZE_EVENT.name
    };

  }];

}]);

/*
* onResize Directive
*
* uses a throttled resize event bound to $window;
* will bind event only once for entire app
* broadcasts 'resize' event from $rootScope
*
* usage: on-resize="expression()"
* event data is available as $event
* $timeout is used to debounce any expressions
* to the end of the current digest
*/
ngResize.directive('onResize', ['$parse', '$timeout', 'myResizeManager', function($parse, $timeout, myResizeManager) {
  return {
    compile: function($element, attr) {
      var fn = $parse(attr.onResize);
      return function(scope, element, attr) {
        scope.$on(myResizeManager.eventName, function(event, data) {
          $timeout(function() {
            scope.$apply(function() {
              fn(scope, { $event: data });
            });
          });
        });
      };
    }
  };
}]);
