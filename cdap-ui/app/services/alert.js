angular.module(PKG.name+'.services')
  .service('myAlert', function(){
    var __list = [];
    function alert(item) {
      if (angular.isObject(item) && Object.keys(item).length) {
        if (__list.length > 0 && __list[0].content === item.content && __list[0].title === item.title) {
          __list[__list.length - 1].count++;
        } else {
          __list.unshift({
            content: item.content,
            title: item.title,
            count: 1
          });
        }
      }
    }

    alert['clear'] = function() {
      __list = [];
    };

    alert['isEmpty'] = function() {
      return __list.length === 0;
    };

    alert['getAlerts'] = function() {
      return __list;
    };

    alert['count'] = function() {
      return __list.length;
    };

    alert['remove'] = function(item) {
      __list.splice(__list.indexOf(item), 1);
    };

    return alert;
  });
