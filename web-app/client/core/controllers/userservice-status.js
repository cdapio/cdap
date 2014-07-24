/*
 * Service Status Controller
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
    needs: ['Userservice'],
    init: function () {
      this.set('expectedPath', 'Userservice.Config');
    },


    load: function () {
    },

    unload: function () {
    }


  });

  Controller.reopenClass({

    type: 'UserserviceStatus',
    kind: 'Controller'

  });

  return Controller;

});