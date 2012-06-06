
define(['views/dashboard', 'views/applications',
	'views/application', 'views/flows',
	'views/flow', 'views/logs', 'views/upload'],
	function (D, As, A, Fs, F, L, U) {
		return {
			Dashboard: D,
			Applications: As,
			Application: A,
			Flows: Fs,
			Flow: F,
			Logs: L,
			Upload: U
		};
	}
);