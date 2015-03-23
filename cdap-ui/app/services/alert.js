angular.module(PKG.name+'.services')
  .service('myAlert', function(){
    var __list = [{title: 500,  content: 'adsdsd'}];
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

    return alert;
  });
