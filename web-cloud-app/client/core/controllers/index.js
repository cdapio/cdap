/*
 * Controller Index
 */

define(['./app', './batch-config', './batch-status',
	'./batch-log', './flow-status',
	'./flow-flowlet', './list', './stream', './dataset',
	'./dashboard', './flow-history', './flow-stream',
	'./flow-config', './procedure-status', './procedure-config',
	'./flow-log', './procedure-log'],

	function () {

		return Array.prototype.slice.call(arguments, 0);

	}
);