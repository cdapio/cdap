/**
 * send socket msgs to the aggregator
 * a new Dispatch is instantiated for each websocket connection
 */

function Dispatch (conn) {
  this.connection = conn;

  console.log('sock start', conn.id);

  conn.on('data', function (message) {

    try {
      message = JSON.parse(message);
      console.log('sock data', conn.id, message);

      conn.write(JSON.stringify({
        response: [Date.now(),Math.random()],
        resource: message.resource
      }));

    }
    catch (e) {}

  });

  conn.on('close', function () {
    console.log('sock closed', conn.id);
  });

}

Dispatch.prototype.doSomething = function () {
  // body...
};

module.exports = Dispatch;

