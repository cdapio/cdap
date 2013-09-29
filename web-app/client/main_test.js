/**
 * Tests for main.
 */

define(['main'], function(Main) {

	describe('Given an application it', function() {

		it ('should be defined', function() {
			expect(Main).toBeDefined();
		});

		it('should have router defined', function() {
			expect(Main.__container__.lookup('router:main').router).toBeDefined();
		});

		it('should have socket and http defined', function() {
			expect(Main.__container__.lookup('Socket:main')).toBeDefined();
			expect(Main.__container__.lookup('HTTP:main')).toBeDefined();
		});

		it('should have routes defined', function() {
			expect(Main.AppRoute).toBeDefined();
			expect(Main.ApplicationRoute).toBeDefined();
			expect(Main.MapReduceLogRoute).toBeDefined();
			expect(Main.MapReduceRoute).toBeDefined();
			expect(Main.MapReduceStatusConfigRoute).toBeDefined();
			expect(Main.MapReduceStatusRoute).toBeDefined();
			expect(Main.MapReduceesRoute).toBeDefined();
			expect(Main.DatasetRoute).toBeDefined();
			expect(Main.DatasetsRoute).toBeDefined();
			expect(Main.FlowHistoryRoute).toBeDefined();
			expect(Main.FlowLogRoute).toBeDefined();
			expect(Main.FlowStatusConfigRoute).toBeDefined();
			expect(Main.FlowStatusFlowletRoute).toBeDefined();
			expect(Main.FlowsRoute).toBeDefined();
			expect(Main.IndexRoute).toBeDefined();
			expect(Main.PageNotFoundRoute).toBeDefined();
			expect(Main.ProcedureLogRoute).toBeDefined();
			expect(Main.ProcedureRoute).toBeDefined();
			expect(Main.ProcedureStatusConfigRoute).toBeDefined();
			expect(Main.ProcedureStatusRoute).toBeDefined();
			expect(Main.ProceduresRoute).toBeDefined();
			expect(Main.StreamRoute).toBeDefined();
			expect(Main.StreamsRoute).toBeDefined();
		});

	});
});