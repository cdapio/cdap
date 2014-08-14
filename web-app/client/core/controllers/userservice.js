/*
 * User service controller.
 */

define(['core/controllers/services'], function (servicesController) {

  var ERROR_TXT = 'Requested Instance count out of bounds.';
  
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
      var btn = this.$().parent().parent().next();
      var inp = this.value;
      if (inp.length > 0 && parseInt(inp) != this.placeholder){
          btn.children().css("opacity",'1')

      } else {
          btn.children().css("opacity",'')
      }
      return true;
    },

    runnableChangeInstances: function (service, runnable) {
      var inputStr = runnable.get('instancesInput');
      var input = parseInt(inputStr);

      runnable.set('instancesInput', '');
      setTimeout(function () {
        $('.services-instances-input').keyup();
      },500);

      if(!inputStr || inputStr.length === 0){
        C.Modal.show('Change Instances','Enter a valid number of instances.');
        return;
      }

      if(isNaN(input) || isNaN(inputStr)){
        C.Modal.show('Incorrect Input', 'Instance count can only be set to numbers (between 1 and 100).');
        return;
      }

      if(service.status !== "RUNNING"){
        //This is because the server would return a 404, if modifying instances while service is off.
        C.Modal.show('Service Stopped', "You can not change the component's instances while its service is stopped.")
        return;
      }

      this.runnableVerifyInstanceBounds(service, runnable.id, input, "Request " + input);
    },

    runnableIncreaseInstance: function (service, runnableID, instanceCount) {
      this.runnableVerifyInstanceBounds(service, runnableID, ++instanceCount, "Increase");
    },
    runnableDecreaseInstance: function (service, runnableID, instanceCount) {
      this.runnableVerifyInstanceBounds(service, runnableID, --instanceCount, "Decrease");
    },

    runnableVerifyInstanceBounds: function (service, runnableID, numRequested, direction) {
      var self = this;
      if (numRequested <= 0) {
        C.Modal.show("Instances Error", ERROR_TXT);
        return;
      }
      C.Modal.show(
        direction + " instances",
        direction + " instances for runnable: " + runnableID + "?",
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
