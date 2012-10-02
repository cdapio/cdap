
define(['views/dashboard', 'views/app', 'views/apps',
	'views/flow', 'views/flowletdetail',
	'views/payload', 'views/visualizer', 'views/dagnode',
	'views/modal', 'views/informer', 'views/chart', 'views/list-page', 'views/flow-list',
	'views/app-list', 'views/stream-list', 'views/dataset-list', 'views/dropzone',
	'views/dataset', 'views/stream', 'views/timeselector', 'views/create-button', 'views/create-dialogue'],
	function (D, A, As, F, Fd, Pl, Vz, Dn, M, I, C, Lp, Fl, Al, Sl, Dsl, Dz, Ds, S, Ts, Cb, Cr) {
		return {
			Dash: D,
			App: A,
			Apps: As,
			Flow: F,
			FlowletDetail: Fd,
			Payload: Pl,
			Visualizer: Vz,
			DagNode: Dn,
			Modal: M,
			Informer: I,
			Chart: C,
			ListPage: Lp,
			FlowList: Fl,
			AppList: Al,
			StreamList: Sl,
			DataSetList: Dsl,
			DropZone: Dz,
			DataSet: Ds,
			Stream: S,
			TimeSelector: Ts,
			CreateButton: Cb,
			Create: Cr
		};
	}
);