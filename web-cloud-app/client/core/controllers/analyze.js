/*
 * Analyze Controller
 */

define([], function (chartHelper) {

  var Controller = Em.Controller.extend(Em.Evented, {
    typesBinding: 'model.types',

    load: function (id) {
      this.set('items', 'asdfsaf');
      this.trigger('loginFailed');
      this.updateData();
    },

    updateData: function () {
      var self = this;
      C.HTTP.get('metrics/events_in?format=rate', function(status, result) {
        self.set('data', result);
        self.set('type', "rate");
      });

    },

    changeChart: function() {
      var self = this;
      if(self.get('type') == 'rate') {
        C.HTTP.get('metrics/events_in?format=count', function(status, result) {
          self.set('data', result);
          self.set('type', 'count');
        });  
      } else {
        this.updateData();
      }
            
    },

    unload: function () {



    },

    delete: function () {


    }

  });

  Controller.reopenClass({
    type: 'Analyze',
    kind: 'Controller'
  });

  return Controller;

});