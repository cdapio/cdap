
define(['models/definition', 'models/flow', 'models/flowlet', 'models/run'],
	function (D, F, Fl, R) {
		return {
			Definition: D,
			Flow: F,
			Flowlet: Fl,
			Run: R
		};
	}
);