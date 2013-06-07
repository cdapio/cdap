/*
 * Elements Result Mock
 */

define([], function () {

	return {
		"App": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "App",
					"name": "My app"
				},
				{
					"id": "DEF",
					"type": "App",
					"name": "My app2"
				}
			]
		},
		"Stream": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "Stream",
					"name": "My Stream",
					"storage": 0
				},
				{
					"id": "DEF",
					"type": "Stream",
					"name": "My Stream2",
					"storage": 0
				}
			]
		},
		"Flow": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "Flow",
					"name": "My Flow"
				}
			]
		},
		"Job": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "Job",
					"name": "My Job"
				}
			]
		},
		"Dataset": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "Dataset",
					"name": "My Dataset",
					"storage": 0
				}
			]
		},
		"Procedure": {
			"status": 200,
			"result": [
				{
					"id": "ABC",
					"type": "Procedure",
					"name": "My Procedure"
				}
			]
		}
	};

});