angular.module(PKG.name+'.services')
  .service('myAlert', function($rootScope){
    var __list = [{title: 404, content: 'Content not found'}];
    function alert(item) {
      if (angular.isObject(item) && Object.keys(item).length) {
        __list.push(item);
      }
    }

    alert['clear'] = function() {
      __list = [];
    }

    alert['isEmpty'] = function() {
      return __list.length === 0;
    }

    alert['getAlerts'] = function() {
      return __list;
    }

    alert['count'] = function() {
      return __list.length;
    };

    return alert;
  });
