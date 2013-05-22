/*
 * Components Index
 */

define(['core/models/index', 'core/controllers/index'],
	function (Models, Controllers) {

		Em.debug('Loading Components');

		return Models.concat(Controllers);

	}
);