import DataSourceConfigurer from '../../services/datasource/DataSourceConfigurer';
import remoteDataSource from './remoteDataSource';
import {apiCreator} from '../../services/resource-helper';
import { USE_REMOTE_SERVER } from './config';

let dataSrc = DataSourceConfigurer.getInstance();

const appPath = '/namespaces/:namespace/apps/ModelRepoMgmtApp';
const servicePath = `${appPath}/spark/ModelRepoMgmtService/methods`;

const MRDSServiceApi = {
  fetchExperimentsDetails: serviceCreator(dataSrc, "GET", "REQUEST",`${servicePath}/experimentsDetails`),
};

function serviceCreator (dataSrc, method, type, path, options = {}) {
  if (USE_REMOTE_SERVER) {
    dataSrc = remoteDataSource;
  }
  return apiCreator(dataSrc, method, type, path, options);
}

export default MRDSServiceApi;
