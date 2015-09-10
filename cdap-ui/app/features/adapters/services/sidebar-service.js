angular.module(PKG.name + '.feature.adapters')
.service('MySidebarService', function() {
  this.isExpanded = true;
  this.isExpandedCallbacks = [];
  this.registerIsExpandedCallback = function(callback) {
    this.isExpandedCallbacks.push(callback);
  };
  this.setIsExpanded = function(value) {
    this.isExpanded = value;
    this.callRegisteredCallbacks();
  };

  this.callRegisteredCallbacks = function() {
    this.isExpandedCallbacks.forEach(function(callback) {
      callback(this.isExpanded);
    }.bind(this));
  };
});