
define(['views/flows',
	'views/flow', 'views/upload', 'views/flowlet', 'views/payload', 'views/visualizer', 'views/dagnode'],
	function (Fs, F, U, Fl, Pl, Vz, Dn) {
		return {
			Flows: Fs,
			Flow: F,
			Upload: U,
			Flowlet: Fl,
			Payload: Pl,
			Visualizer: Vz,
			DagNode: Dn
		};
	}
);