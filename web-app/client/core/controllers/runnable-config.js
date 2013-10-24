/*
 * Runnable Config Controller
 * Runnables (Flow, Mapreduce, Procedure) extend this for Config functionality.
 */

define([], function () {

	var Controller = Em.Controller.extend({

		load: function () {

			var parent = this.get('needs')[0];
			var model = this.get('controllers').get(parent).get('model');
			var list = Em.ArrayProxy.create({ content: [] });

			this.set('config', list);
			this.set('model', model);

			var self = this;

			this.HTTP.get('rest', 'apps', model.get('app'),
				model.get('plural').toLowerCase(),
				model.get('name'), 'runtimeargs', function (args) {

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

		unload: function () {

		},

		runnable: function () {

			if (this.get('model.type') === 'Workflow') {
				return false;
			}
			return true;

		}.property('model'),

		add: function () {

			var key = $('.config-editor-new .config-editor-key input').val();
			var value = $('.config-editor-new .config-editor-value input').val();

			if (key && value) {

				$('.config-editor-new .config-editor-key input').val('');
				$('.config-editor-new .config-editor-value input').val('');

				this.addDone(key, value);

			}

		},

		addDone: function (key, value) {

			this.get('config').pushObject({
				key: key,
				value: value
			});

			$('.config-editor-new .config-editor-key input').select();

		},

		remove: function (view) {

			var index = view.contentIndex;
			this.get('config').removeAt(index);

			/*
			 * Call rerender() ro reset the 'contentIndex' values once removed.
			 */
			view.get('parentView').get('parentView').rerender();

		},

		currentEdit: null,

		edit: function (kind, index) {

			if (this.currentEdit) {
				this.currentEdit.removeClass('editing');
			}

			var children = $('.config-editor .config-editor-' + kind);
			$(children[index]).addClass('editing');
			$(children[index]).find('input').select();

			this.currentEdit = $(children[index]);

		},

		editDone: function (kind, index) {

			var children = $('.config-editor .config-editor-' + kind);
			$(children[index]).removeClass('editing');
			$(children[index]).find('input').select();

		},

		clear: function () {

			this.set('config', []);

			$('.config-editor-new .config-editor-key input').select();

		},

		save: function () {

			var config = {};
			var model = this.get('model');

			this.get('config').forEach(function (item) {
				config[item.key] = item.value;
			});

			config = JSON.stringify(config);

			this.HTTP.put('rest', 'apps', model.get('app'),
				model.get('plural').toLowerCase(),
				model.get('name'), 'runtimeargs', {
					data: config
				}, function () {

					// Noop

			});

			this.close();

		},

		runOnce: function () {

			var config = {};
			this.get('config').forEach(function (item) {
				config[item.key] = item.value;
			});

			var model = this.get('model');
			model.startWithConfig(this.HTTP, config);

			this.close();

		},

		saveAndRun: function () {

			this.save();
			this.runOnce();

		},

		saveAndRestart: function () {

			this.save();

			var program = this.get('model'), self = this;

			program.stop(this.HTTP, function () {
				self.runOnce();
			});

		},

		close: function () {

			var parent = this.get('needs')[0];
			var model = this.get('controllers').get(parent).get('model');

			/*
			 * HAX: The URL route needs the ID of a runnable to be app_id:flow_id.
			 * However, Ember is smart enough to not reload the parent controller.
			 * Therefore, the "correct" ID is preserved on the parent controller's model.
			 */
			if (model.id && model.id.indexOf(':') === -1) {
				model.id = model.app + ':' + model.id;
			}

			this.transitionToRoute(parent, model);

		}
	});

	Controller.reopenClass({

		type: 'RunnableConfig',
		kind: 'Controller'

	});

	return Controller;

});