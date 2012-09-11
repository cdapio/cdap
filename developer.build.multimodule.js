({
    baseUrl: "web-cloud-app/client/developer",
    locale: "en-us",  
    inlineText: true,
    out: 'web-cloud-app/build/developer/client/main.js',
    name: 'main',
    paths: {
    	"core": "../core/",
		"lib": "../core/lib",
		"models": "../core/models",
		"views": "../core/views",
		"controllers": "../core/controllers",
		"partials": "../core/partials"
	}
})