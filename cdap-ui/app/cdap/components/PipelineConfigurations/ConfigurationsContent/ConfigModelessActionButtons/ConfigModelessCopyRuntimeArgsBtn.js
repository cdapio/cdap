/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import isEmpty from 'lodash/isEmpty';
import Clipboard from 'clipboard';

const mapStateToProps = (state, ownProps) => {
  return {
    runtimeArgs: state.runtimeArgs,
    setRuntimeArgsCopiedState: ownProps.setRuntimeArgsCopiedState,
    runtimeArgsCopied: ownProps.runtimeArgsCopied
  };
};

const ConfigModelessCopyRuntimeArgsComp = ({runtimeArgs, setRuntimeArgsCopiedState, runtimeArgsCopied}) => {
  let runtimeArgsObj = convertKeyValuePairsObjToMap(runtimeArgs);
  let noRuntimeArgs = isEmpty(runtimeArgsObj);

  return (
    <button
      className="btn btn-primary apply-action"
      disabled={runtimeArgsCopied || noRuntimeArgs}
      onClick={setRuntimeArgsCopiedState}
      data-clipboard-text={JSON.stringify(runtimeArgsObj)}
    >
      <span>
        {
          runtimeArgsCopied ?
            'Runtime Arguments Copied'
          :
            'Copy Runtime Arguments'
        }
      </span>
    </button>
  );
};

ConfigModelessCopyRuntimeArgsComp.propTypes = {
  runtimeArgs: PropTypes.object,
  setRuntimeArgsCopiedState: PropTypes.func,
  runtimeArgsCopied: PropTypes.bool
};

const ConnectedConfigModelessCopyRuntimeArgsComp = connect(mapStateToProps)(ConfigModelessCopyRuntimeArgsComp);

export default class ConfigModelessCopyRuntimeArgsBtn extends Component {
  static propTypes = {
    setRuntimeArgsCopiedState: PropTypes.func,
    runtimeArgsCopied: PropTypes.bool
  };

  componentDidMount() {
    new Clipboard('.apply-action');
  }

  render() {
    return (
      <ConnectedConfigModelessCopyRuntimeArgsComp
        setRuntimeArgsCopiedState={this.props.setRuntimeArgsCopiedState}
        runtimeArgsCopied={this.props.runtimeArgsCopied}
      />
    );
  }
}


