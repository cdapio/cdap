/*
 * Dataset Controller
 */

define([], function () {

    var Controller = Em.Controller.extend({

        load: function () {


        },

        unload: function () {

        }

    });

    Controller.reopenClass({
        type: 'Test',
        kind: 'Controller'
    });

    return Controller;

});
