
define([], function () {

  return Em.View.extend({
    remainingBinding: 'App.Controllers.Todos.remaining',

    remainingString: function() {
      var remaining = this.get('remaining');
      return remaining + (remaining === 1 ? " item" : " items");
    }.property('remaining')

  });

});
