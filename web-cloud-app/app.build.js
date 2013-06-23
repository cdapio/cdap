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
        "embeddables": "../core/embeddables",
		"controllers": "../core/controllers",
		"partials": "../core/partials"
	},
    wrap: {
        start: "var copyright = 'Copyright (c) 2013 Continuuity, Inc. Minified using RequireJS. https://github.com/jrburke/requirejs';",
        end: "var thanks = 'Thanks for using Continuuity.';"
    },
    preserveLicenseComments: false
})