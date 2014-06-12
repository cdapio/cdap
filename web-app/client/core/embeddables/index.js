/*
 * core/embeddables Index
 */

define([
  'core/embeddables/breadcrumb', 'core/embeddables/chart', 'core/embeddables/create-button',
  'core/embeddables/key-val', 'core/embeddables/create-dialogue', 'core/embeddables/textfield',
  'core/embeddables/dagnode', 'core/embeddables/dropzone', 'core/embeddables/injector',
  'core/embeddables/modal', 'core/embeddables/timeselector', 'core/embeddables/visualizer',
  'core/embeddables/dash-chart', 'core/embeddables/analyze',
  'core/embeddables/empty-dagnode', 'core/embeddables/login'
  ], function () {

		Em.debug('Loading core/embeddables');

		return Array.prototype.slice.call(arguments, 0);

	}
);