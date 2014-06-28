/*
 * Controller Index
 */

define(['core/controllers/app', 'core/controllers/mapreduce-config', 'core/controllers/mapreduce-status',
	'core/controllers/mapreduce-log', 'core/controllers/mapreduce-history', 'core/controllers/flow-status',
	'core/controllers/flow-flowlet', 'core/controllers/list', 'core/controllers/stream',
  'core/controllers/dataset', 'core/controllers/overview', 'core/controllers/resources',
  'core/controllers/flow-history',
  'core/controllers/flow-stream', 'core/controllers/flow-config',
  'core/controllers/procedure-status', 'core/controllers/procedure-config',
	'core/controllers/flow-log', 'core/controllers/procedure-log', 'core/controllers/analyze',
  'core/controllers/workflow-status', 'core/controllers/workflow-history',
  'core/controllers/workflow-config', 'core/controllers/login', 'core/controllers/loading',
  'core/controllers/services', 'core/controllers/service-log', 'core/controllers/connection-error',
  'core/controllers/accesstoken'],
	function () {

		return Array.prototype.slice.call(arguments, 0);

	}
);