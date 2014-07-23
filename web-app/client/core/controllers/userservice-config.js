/*
 * Service Config Controller
 */

define(['core/controllers/runnable-config'], function (RunnableConfigController) {

  var Controller = RunnableConfigController.extend({

    /*
     * This syntax makes the Service controller available to this controller.
     * This allows us to access the service model that has already been loaded.
     *
     * RunnableConfigController uses this value to do its work. Take a look there.
     *
     */
    needs: ['UserService'],
    init: function () {
      this.set('expectedPath', 'UserService.Config');
    }

  });

  Controller.reopenClass({

    type: 'UserserviceConfig',
    kind: 'Controller'

  });

  return Controller;

});