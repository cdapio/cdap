var log4js = require('./lib/log4js'),
    logger = log4js.getLogger('stress-test'),
    cluster = require('cluster'),
    numCPUs = require('os').cpus().length;

if (cluster.isMaster) {
    console.log("Starting master");
    // Fork workers.
    for (var i = 0; i < numCPUs; i++) {
        console.log("Forked worker %d of %d", i + 1, numCPUs);
        cluster.fork();
    }

    log4js.configure({
        "appenders": [{
            "type": "multiprocess",
            "mode": "master",
            "loggerPort": 12345,
            "appender": {
                "type": "console"
            }
        }]
    });

    cluster.on('death', function(worker) {
        console.log('worker ' + worker.pid + ' died');
    });
} else {
    log4js.configure({
        appenders: [{
            "type": "multiprocess",
            "mode": "worker",
            "loggerPort": 12345
        }]
    });

    console.log("Starting a worker");

    for (var j=0; j < 10000; j++) {
        function log(value) {
            logger.debug("This will eventually break stuff: %d", value);
        }
        setTimeout(log.bind(null, j), Math.random() * 5000);
    }
}
