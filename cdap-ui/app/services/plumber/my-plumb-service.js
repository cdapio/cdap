angular.module(PKG.name + '.services')
  .service('MyPlumbService', function() {
    this.callbacks = [];
    this.config = {
      source: {},
      sink: {},
      transforms: []
    };

    this.registerCallBack = function (callback) {
      this.callbacks.push(callback);
    };

    this.updateConfig = function(conf, type) {
      switch(type) {
        case 'source':
          this.config.source = conf;
          break;
        case 'sink':
          this.config.sink = conf;
          break;
        case 'transform':
          this.config.transforms.push(conf);
          break;
      }
      console.info('Config', this.config);
      this.notifyListeners(conf, type);
    };

    this.notifyListeners = function(conf, type) {
      this.callbacks.forEach(function(callback) {
        callback(conf, type);
      });
    };


  });
