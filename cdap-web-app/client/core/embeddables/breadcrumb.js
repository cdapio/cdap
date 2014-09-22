/*
 * Breadcrumb Embeddable
 */

define([], function () {

	var Embeddable = Em.View.extend({
		template: Em.Handlebars.compile('{{#each view.breadcrumbs}}<li><a {{bindAttr href="href"}}>{{name}}</a></li>{{/each}}'),
		tagName: 'ul',
		elementId: 'breadcrumb',
		didInsertElement: function () {

			var names = {
				'flows': 'Process',
				'upload': 'Upload',
				'apps': 'Applications',
				'streams': 'Collect',
				'datasets': 'Store'
			};

			var self = this;

			this.set('breadcrumbs', Em.ArrayProxy.extend({
				content: function () {

					var path = window.location.hash.split('/');

					/** Hax. Ignore Flow routes to simplify. **/
					if (path.length > 3 && path[2].indexOf(':') !== -1) {
						path = path.slice(0, 3);
					}

					var crumbs = [];
					var href = ['#'];

					/** Hax. Deals with AppID:FlowID style IDs for flows. **/
					if (path.length && path[path.length - 1].indexOf(':') !== -1) {

						var app = path[path.length - 1].split(':')[0];
						var flow = path[path.length - 1].split(':')[1];
						return [
							{
								name: names[app] || app,
								href: '#/apps/' + app
							}
						];
					}
					/** Hax. If it's an App, don't link to AppList. **/
					if (path[1] === 'apps') {
						return [];
					}
					/** End Hax. **/

					for (var i = 1; i < path.length - 1; i ++) {
						href.push(path[i]);
						crumbs.push({
							name: names[path[i]] || path[i],
							href: href.join('/')
						});
					}

					return crumbs;

				}.property('C.currentPath')
			}).create());

		}
	});

	Embeddable.reopenClass({
		type: 'Breadcrumb',
		kind: 'Embeddable'
	});

	return Embeddable;

});