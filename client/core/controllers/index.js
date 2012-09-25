
define(['core/controllers/app', 'core/controllers/flows', 'core/controllers/flow',
	'core/controllers/upload', 'core/controllers/list',
	'core/controllers/stream', 'core/controllers/dataset',
	'core/controllers/dashboard'],
	function (A, Fs, F, U, L, St, Ds, Da) {
		return {
			App: A,
			Flows: Fs,
			Flow: F,
			Upload: U,
			List: L,
			Stream: St,
			DataSet: Ds,
			Dashboard: Da
		};
	}
);