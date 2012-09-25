
define(['views/dashboard', 'views/app', 'views/flows',
	'views/flow', 'views/flowlet',
	'views/payload', 'views/visualizer', 'views/dagnode',
	'views/modal', 'views/informer', 'views/sparkline'],
	function (D, A, Fs, F, Fl, Pl, Vz, Dn, M, I, S) {
		return {
			Dash: D,
			App: A,
			Flows: Fs,
			Flow: F,
			Flowlet: Fl,
			Payload: Pl,
			Visualizer: Vz,
			DagNode: Dn,
			Modal: M,
			Informer: I,
			Sparkline: S
		};
	}
);