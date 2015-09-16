angular.module(PKG.name + '.feature.adapters')
  .service('MyConsoleTabService', function() {
    this.messages = [];
    this.resetMessages = function() {
      this.messages = [];
    };

    this.onMessageUpdateListeners = [];
    this.addMessage = function(message) {
      this.messages.push(message);
      this.notifyMessageUpdateListeners(message);
    };

    this.registerOnMessageUpdates = function(callback) {
      this.onMessageUpdateListeners.push(callback);
    };

    this.notifyMessageUpdateListeners = function(message) {
      this.onMessageUpdateListeners.forEach(function(callback) {
        callback(message);
      });
    };

  });
