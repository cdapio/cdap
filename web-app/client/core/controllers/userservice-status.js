/*
 * Userservice Status Controller
 */

define(['core/controllers/userservice'], function (Userservice) {

  var Controller = Userservice.extend({

    /*
     * This syntax makes the Userservice controller available to this controller.
     * This allows us to access the service model that has already been loaded.
     *
     */
    needs: ['Userservice'],
    init: function () {
      this.set('expectedPath', 'UserserviceStatus');
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