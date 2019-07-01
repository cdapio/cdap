const request = require('request');

function getGETRequestOptions() {
    return {
        method: 'GET',
        json: true
    }
}

function requestPromiseWrapper(options) {
    return new Promise((resolve, reject) => {
        request(options, (err, response, body) => {
            if (err) {
                return reject(err);
            }

            return resolve(body);
        });
    });
};

module.exports = {
    getGETRequestOptions,
    requestPromiseWrapper
}
