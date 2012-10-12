
define(['models/app', 'models/definition', 'models/flow', 'models/flowlet', 
	'models/run', 'models/stream', 'models/dataset', 'models/query'],
	function (A, D, F, Fl, R, S, Ds, Q) {
		return {
			Application: A,
			Definition: D,
			Flow: F,
			Flowlet: Fl,
			Run: R,
			Stream: S,
			Dataset: Ds,
			Query: Q
		};
	}
);