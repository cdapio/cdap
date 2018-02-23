/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
*/

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { connect, Provider } from 'react-redux';
import MicroserviceQueueActions from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueActions';
import {createMicroserviceQueueStore} from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueStore';
import MicroserviceQueue from 'components/CaskWizards/MicroserviceUpload/MicroserviceQueue';
import IconSVG from 'components/IconSVG';
import uuidV4 from 'uuid/v4';
import classnames from 'classnames';

require('./MicroserviceQueue.scss');

const mapStateToFieldNameProps = (state, ownProps) => {
  return {
    name: state.general.queues[ownProps.index].name,
    type: state.general.queues[ownProps.index].type,
    namespace: state.general.queues[ownProps.index].properties.namespace,
    topic: state.general.queues[ownProps.index].properties.topic,
    region: state.general.queues[ownProps.index].properties.region,
    accessKey: state.general.queues[ownProps.index].properties['access-key'],
    accessId: state.general.queues[ownProps.index].properties['access-id'],
    queueName: state.general.queues[ownProps.index].properties['queue-name'],
    endpoint: state.general.queues[ownProps.index].properties.endpoint,
    connection: state.general.queues[ownProps.index].properties.connection,
    sslKeystoreFilePath: state.general.queues[ownProps.index].properties['ssl-keystore-file-path'],
    sslKeystorePassword: state.general.queues[ownProps.index].properties['ssl-keystore-password'],
    sslKeystoreKeyPassword: state.general.queues[ownProps.index].properties['ssl-keystore-key-password'],
    sslKeystoreType: state.general.queues[ownProps.index].properties['ssl-keystore-type'],
    sslTruststoreFilePath: state.general.queues[ownProps.index].properties['ssl-truststore-file-path'],
    sslTruststorePassword: state.general.queues[ownProps.index].properties['ssl-truststore-password'],
    sslTruststoreType: state.general.queues[ownProps.index].properties['ssl-truststore-type'],
    mapRTopic: state.general.queues[ownProps.index].properties.mapRTopic,
    keySerdes: state.general.queues[ownProps.index].properties['key-serdes'],
    valueSerdes: state.general.queues[ownProps.index].properties['value-serdes']
  };
};

const fieldToActionMap = {
  name: MicroserviceQueueActions.setName,
  type: MicroserviceQueueActions.setType,
  namespace: MicroserviceQueueActions.setNamespace,
  topic: MicroserviceQueueActions.setTopic,
  region: MicroserviceQueueActions.setRegion,
  accessKey: MicroserviceQueueActions.setAccessKey,
  accessId: MicroserviceQueueActions.setAccessId,
  queueName: MicroserviceQueueActions.setQueueName,
  endpoint: MicroserviceQueueActions.setEndpoint,
  connection: MicroserviceQueueActions.setConnection,
  sslKeystoreFilePath: MicroserviceQueueActions.setSSLKeystoreFilePath,
  sslKeystorePassword: MicroserviceQueueActions.setSSLKeystorePassword,
  sslKeystoreKeyPassword: MicroserviceQueueActions.setSSLKeystoreKeyPassword,
  sslKeystoreType: MicroserviceQueueActions.setSSLKeystoreType,
  sslTruststoreFilePath: MicroserviceQueueActions.setSSLTruststoreFilePath,
  sslTruststorePassword: MicroserviceQueueActions.setSSLTruststorePassword,
  sslTruststoreType: MicroserviceQueueActions.setSSLTruststoreType,
  mapRTopic: MicroserviceQueueActions.setMapRTopic,
  keySerdes: MicroserviceQueueActions.setKeySerdes,
  valueSerdes: MicroserviceQueueActions.setValueSerdes
};

const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    onRemove: () => {
      dispatch({
        type: MicroserviceQueueActions.deleteQueue,
        payload: {index: ownProps.index}
      });
    },
    onAdd: () => {
      dispatch({
        type: MicroserviceQueueActions.addQueue,
        payload: {index: ownProps.index}
      });
    },
    onChange: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          [fieldProp]: e.target.value
        }
      });
    }
  };
};

let MicroserviceQueueCopy = connect(
  mapStateToFieldNameProps,
  mapDispatchToFieldNameProps
)(MicroserviceQueue);

export default class MicroserviceQueueEditor extends Component {
  static propTypes = {
    values: PropTypes.shape({
      queues: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string,
        type: PropTypes.string,
        properties: PropTypes.object
      }))
    }),
    onChange: PropTypes.func,
    className: PropTypes.string
  };

  state = {
    queues: this.props.values
  };

  constructor(props) {
    super(props);
    let { values, onChange } = this.props;

    this.MicroserviceQueueStore = createMicroserviceQueueStore({values});

    this.sub = this.MicroserviceQueueStore.subscribe(() => {
      let queues = this.MicroserviceQueueStore.getState().general.queues;
      onChange(queues);
      this.setState({ queues });
    });

    this.MicroserviceQueueStore.dispatch({
      type: MicroserviceQueueActions.onUpdate,
      payload: {
        queues: values
      }
    });
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      queues: [...nextProps.values]
    });
  }
  shouldComponentUpdate(nextProps) {
    return this.state.queues.length !== nextProps.values.length;
  }
  componentWillUnmount() {
    this.sub();
  }

  addFirstQueue = () => {
    this.MicroserviceQueueStore.dispatch({
      type: MicroserviceQueueActions.addQueue,
      payload: { index: 0 }
    });
  }

  render() {
    if (!this.state.queues || !this.state.queues.length) {
      return (
        <div
          className={classnames(this.props.className, "microservice-add-first-queue-container")}
          onClick={this.addFirstQueue}
        >
          <button className="btn add-first-queue-btn btn-link">
            <IconSVG name="icon-plus" />
          </button>
        </div>
      );
    }
    return (
      <Provider store={this.MicroserviceQueueStore} key={uuidV4()}>
        <div className="microservice-queue-editor-container">
          {
            this.state.queues.map((field, index) => {
              return (
                <MicroserviceQueueCopy
                  index={index}
                />
              );
            })
          }
        </div>
      </Provider>
    );
  }
}
