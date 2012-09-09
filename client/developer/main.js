
require.config({
	paths: {
		"core": "../core/"
	}
});

define (['core/app', 'router', 'patch/views/index'], function (App, Router, ViewPatch) {
	
	App.Context = {
		Views: ViewPatch
	};

	$('#product-edition').html("Developer Edition");

	App.router = new Router(App.Views);
	App.Connect();

});