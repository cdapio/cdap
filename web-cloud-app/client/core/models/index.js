/*
 * Models Index
 */

define(['models/app', 'models/flow', 'models/flowlet', 'models/batch',
	'models/run', 'models/stream', 'models/dataset', 'models/procedure', 'models/queue'],
	function () {

		return Array.prototype.slice.call(arguments, 0);

	}
);