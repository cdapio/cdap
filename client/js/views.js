
define(['views/flows',
	'views/flow', 'views/upload'],
	function (Fs, F, U) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U
		};
	}
);