
define(['models/definition', 'models/flow', 'models/flowlet', 'models/run', 'models/stream'],
	function (D, F, Fl, R, S) {
		return {
			Definition: D,
			Flow: F,
			Flowlet: Fl,
			Run: R,
			Stream: S
		};
	}
);