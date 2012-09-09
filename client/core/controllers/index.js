
define(['core/controllers/flows', 'core/controllers/flow', 'core/controllers/upload'],
	function (Fs, F, U) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U
		};
	}
);