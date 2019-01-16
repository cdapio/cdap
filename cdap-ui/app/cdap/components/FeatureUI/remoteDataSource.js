import { Observable } from 'rxjs/Observable';
const REMOTE_IP = "http://192.168.156.36:11015";

class RemoteDataSource {
  request(reqObj) {
    return Observable.fromPromise(fetch(this.getFetchUrl(reqObj), this.getFetchObject(reqObj)).then(res => res.json()));
  }

  poll(reqObj) {
    return Observable.fromPromise(fetch(reqObj._cdapPath).then(res => res.json()));
  }

  getFetchUrl(request) {
    return REMOTE_IP + "/v3" + request._cdapPath;
  }

  getFetchObject(request) {
    let fetchObject = {};
    switch (request.method) {
      case "POST":
        fetchObject = {
          method: 'POST',
          body: JSON.stringify(request.body)
        };
        break;
      case "DELETE":
        fetchObject = {
          method: 'DELETE',
          body: ""
        };
        break;
      case "GET":
        fetchObject = {};
        break;
      case "PUT":
        fetchObject = {
          method: 'PUT',
          body: JSON.stringify(request.body)
        };
        break;
    }
    return fetchObject;
  }
}
const remoteDataSource = new RemoteDataSource();
export default remoteDataSource;


