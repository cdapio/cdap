
define(['views/flows',
	'views/flow', 'views/upload', 'views/flowlet'],
	function (Fs, F, U, Fl) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U,
			Flowlet: Fl
		};
	}
);