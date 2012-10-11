
define(['core/controllers/app', 'core/controllers/flow',
	'core/controllers/upload', 'core/controllers/list',
	'core/controllers/stream', 'core/controllers/dataset',
	'core/controllers/dashboard', 'core/controllers/flow-history'],
	function (A, F, U, L, St, Ds, Da, Fh) {
		return {
			Application: A,
			Flow: F,
			Upload: U,
			List: L,
			Stream: St,
			Dataset: Ds,
			Dashboard: Da,
			FlowHistory: Fh
		};
	}
);