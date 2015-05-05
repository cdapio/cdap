angular.module(PKG.name+'.services')
  .service('MyOrderings', function(myLocalStorage) {
    var APP_KEY = 'appOrdering';
    var DATA_KEY = 'dataOrdering';
    myLocalStorage.get(APP_KEY).then(function(value) {
      if (typeof value === 'undefined') {
        this.appList = [];
      } else {
        this.appList = value;
      }
    }.bind(this));
    myLocalStorage.get(DATA_KEY).then(function(value) {
      if (typeof value === 'undefined') {
        this.dataList = [];
      } else {
        this.dataList = value;
      }
    }.bind(this));

    function typeClicked(arr, id, key) {
      // delay by 1000ms so that the order does not change on the same visit to the page.
      setTimeout(function() {
        var idx = arr.indexOf(id);
        if (idx !== -1) {
          arr.splice(idx, 1);
        }
        arr.unshift(id);
        myLocalStorage.set(key, arr);
      }, 1000);
    }

    this.appClicked = function (appName) {
      typeClicked(this.appList, appName, APP_KEY);
    };

    this.dataClicked = function (dataName) {
      typeClicked(this.dataList, dataName, DATA_KEY);
    };

    function typeOrdering(arr, el) {
      var idx = arr.indexOf(el.name);
      if (idx === -1) {
        return arr.length;
      }
      return idx;
    }

    this.appOrdering = function(app) {
      return typeOrdering(this.appList, app);
    }.bind(this);

    this.dataOrdering = function(data) {
      return typeOrdering(this.dataList, data);
    }.bind(this);
  });
