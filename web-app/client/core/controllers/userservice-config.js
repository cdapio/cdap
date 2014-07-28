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
    needs: ['Userservice'],
    init: function () {
      this.set('expectedPath', 'Userservice.Config');
    },


    load: function () {
      var parent = this.get('needs')[0];
      var model = this.get('controllers').get(parent).get('model');
      var list = Em.ArrayProxy.create({ content: [] });

      this.set('config', list);
      this.set('model', model);

      var self = this;

      this.HTTP.get('rest', 'apps', model.get('app'),
        'services', model.get('name'), 'runtimeargs', function (args) {
          var config = [];
          for (var key in args) {
            config.push({
              key: key,
              value: args[key]
            });
          }

          self.get('config').pushObjects(config);

      });

      Em.run.next(function () {
        $('.config-editor-new .config-editor-key input').select();
      });
    },


    save: function () {

      var config = {};
      var model = this.get('model');

      this.get('config').forEach(function (item) {
        config[item.key] = item.value;
      });

      config = JSON.stringify(config);

      this.HTTP.put('rest', 'apps', model.get('app'),
        'services',
        model.get('name'), 'runtimeargs', {
          data: config
        }, function () {} //noop
      );

      this.close();

    },

    close: function () {
      this.transitionToRoute("UserserviceStatus", this.get('model'));
    }


  });

  Controller.reopenClass({

    type: 'UserserviceStatusConfig',
    kind: 'Controller'

  });

  return Controller;

});