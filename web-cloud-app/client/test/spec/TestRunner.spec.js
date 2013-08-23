
require.config({
		urlArgs : 'bust=',
		baseUrl: '../../',
		paths: {
			"jasmine-jquery": 'core/lib/jasmine-jquery',
			'jquery' : 'core/lib/jquery-1.9.1',
    	'handlebars' : 'core/lib/handlebars',
			'ember' : 'core/lib/ember-1.0.0-rc.5',
			'jsPlumb': 'core/lib/jquery.jsPlumb-1.3.6-all',
			'jqueryTimeago': 'core/lib/jquery.timeago',
			'socket': 'core/lib/socket.io'
		},
		shim: {
			'jsPlumb': ['jquery'],
			'jqueryTimeago': ['jquery']
		}
		
});

require(
	[
		'jasmine-jquery',
		'jquery',
		'handlebars',
		'ember',
		'jsPlumb',
		'jqueryTimeago',
		'socket',

		//Tests
		'main_test',
		'core/application_test',
		'core/util_test',
		'core/http_test'
	], function() {

	Ember.testing = true;

	var jasmineEnv = jasmine.getEnv();
	jasmineEnv.updateInterval = 1000;

	var trivialReporter = new jasmine.TrivialReporter();

	jasmineEnv.addReporter(trivialReporter);

	jasmineEnv.specFilter = function(spec) {
		return trivialReporter.specFilter(spec);
	};

	Ember.run(function() {
		jasmineEnv.execute();
	});

});
