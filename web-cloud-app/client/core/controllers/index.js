/*
 * Controller Index
 */

define(['core/controllers/app', 'core/controllers/batch-config', 'core/controllers/batch-status',
	'core/controllers/batch-log', 'core/controllers/flow-status',
	'core/controllers/flow-flowlet', 'core/controllers/list', 'core/controllers/stream',
  'core/controllers/dataset', 'core/controllers/dashboard', 'core/controllers/flow-history',
  'core/controllers/flow-stream', 'core/controllers/flow-config',
  'core/controllers/procedure-status', 'core/controllers/procedure-config',
	'core/controllers/flow-log', 'core/controllers/procedure-log'],

	function () {

		return Array.prototype.slice.call(arguments, 0);

	}
);