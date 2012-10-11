
define(['models/app', 'models/definition', 'models/flow', 'models/flowlet', 'models/run', 'models/stream', 'models/dataset'],
	function (A, D, F, Fl, R, S, Ds) {
		return {
			Application: A,
			Definition: D,
			Flow: F,
			Flowlet: Fl,
			Run: R,
			Stream: S,
			Dataset: Ds
		};
	}
);