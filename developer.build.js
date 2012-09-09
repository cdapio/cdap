({
    baseUrl: "client/developer",
    locale: "en-us",  
    inlineText: true,
    out: './build/developer/client/main.js',
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