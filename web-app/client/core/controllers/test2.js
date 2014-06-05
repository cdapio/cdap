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
        type: 'Test2',
        kind: 'Controller'
    });

    return Controller;

});
