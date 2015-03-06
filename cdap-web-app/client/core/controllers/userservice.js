/*
 * User service controller.
 */

define(['core/controllers/services'], function (servicesController) {

  var Controller = servicesController.extend({
    needs: ['Userservice'],

    config: function () {
      var model = this.get('model');
      this.transitionToRoute('UserserviceStatus.Config');
    },

    load: function () {
      var self = this;
      var parent = this.get('needs')[0];
      var model = this.get('controllers').get(parent).get('model');


      this.interval = setInterval(function () {
        if (model) {
          model.update(self.HTTP);
        }
      }, C.POLLING_INTERVAL)
    },

    unload: function () {

      clearInterval(this.interval);

    },

    keyPressed: function (evt) {
      var btn = this.$().parent().parent().next().children();
      var inp = this.value;
      return C.Util.handleInstancesKeyPress(btn, inp, this.placeholder);
    },

    runnableChangeInstances: function (service, runnable) {
      var inputStr = runnable.get('instancesInput');
      var input = parseInt(inputStr);

      runnable.set('instancesInput', '');
      setTimeout(function () {
        $('.services-instances-input').keyup();
      },500);

      if (runnable && runnable.requested === input) {
        return; //no-op
      }

      this.setInstances(service, runnable.id, input);
    },

    setInstances: function (service, runnableID, numRequested) {
      var self = this;
      C.Modal.show(
        "Request " + numRequested + " instances",
        "Request " + numRequested + " instances for runnable: " + runnableID + "?",
        function () {
          var url = 'rest/apps/' + service.app + '/services/' + service.name
              + '/runnables/' + runnableID + '/instances';
          var callback =  function(){service.update(self.HTTP)};
          self.executeInstanceCall(url, numRequested, callback);
        }
      );
    },


    exec: function () {

      var model = this.get('model');
      var action = model.get('defaultAction');
      if (action && action.toLowerCase() in model) {
        model[action.toLowerCase()](this.HTTP);
      }

    },

  });

  Controller.reopenClass({
    type: 'Userservice',
    kind: 'Controller'
  });

  return Controller;

});
