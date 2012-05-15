
define(['views/list', 'views/create', 'views/stats'],
	function (ListView, CreateView, StatsView) {
		return {
			List: ListView,
			Create: CreateView,
			Stats: StatsView
		};
	}
);