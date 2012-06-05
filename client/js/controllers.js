
define(['controllers/applications', 'controllers/application', 'controllers/flows', 'controllers/flow'],
	function (As, A, Fs, F) {
		return {
			Applications: As,
			Application: A,
			Flows: Fs,
			Flow: F
		};
	}
);