
define(['views/flows',
	'views/flow', 'views/upload', 'views/flowlet', 'views/payload', 'views/visualizer', 'views/dagnode', 'views/modal', 'views/informer'],
	function (Fs, F, U, Fl, Pl, Vz, Dn, M, I) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U,
			Flowlet: Fl,
			Payload: Pl,
			Visualizer: Vz,
			DagNode: Dn,
			Modal: M,
			Informer: I
		};
	}
);