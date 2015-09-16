angular.module(PKG.name + '.feature.adapters')
  .controller('ConsoleTabController', function(MyConsoleTabService) {
    this.messages = MyConsoleTabService.messages;
    this.clearMessages = function() {
      MyConsoleTabService.resetMessages();
      this.messages = MyConsoleTabService.messages;
    };
  });
