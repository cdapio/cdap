/*
 * Components Index
 */

define(['core/models/index', 'core/controllers/index', 'core/embeddables/index'],
	function (Models, Controllers, Embeddables) {

		Em.debug('Loading Components');

		return Models.concat(Controllers).concat(Embeddables);

	}
);