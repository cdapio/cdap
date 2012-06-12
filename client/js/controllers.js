
define(['controllers/flows', 'controllers/flow', 'controllers/upload'],
	function (Fs, F, U) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U
		};
	}
);