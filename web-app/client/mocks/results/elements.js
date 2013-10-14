/*
 * Elements Result Mock
 */

define([], function () {

	return {
		"App": {
			"status": 200,
			"result": [
				{
					"id": "appid1",
					"type": "App",
					"name": "My app"
				},
				{
					"id": "appid2",
					"type": "App",
					"name": "My app2"
				}
			]
		},
		"Stream": {
			"status": 200,
			"result": [
				{
					"id": "streamid1",
					"type": "Stream",
					"name": "My Stream",
					"storage": 0
				},
				{
					"id": "streamid2",
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
					"id": "flowid1",
					"type": "Flow",
					"name": "My Flow",
					"application": "SampleApplication",
					"applicationId": "SampleApplicationId"
				}
			]
		},
		"MapReduce": {
			"status": 200,
			"result": [
				{
					"id": "batchid1",
					"type": "MapReduce",
					"name": "My MapReduce",
					"application": "SampleApplication",
					"applicationId": "SampleApplicationId"
				}
			]
		},
		"Job": {
			"status": 200,
			"result": [
				{
					"id": "jobid1",
					"type": "Job",
					"name": "My Job"
				}
			]
		},
		"Dataset": {
			"status": 200,
			"result": [
				{
					"id": "datasetid1",
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
					"id": "procedureid1",
					"type": "Procedure",
					"name": "My Procedure",
					"application": "SampleApplication",
					"applicationId": "SampleApplicationId"
				}
			]
		}
	};

});