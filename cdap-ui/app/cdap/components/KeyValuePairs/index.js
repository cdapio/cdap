/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

/*
  FIXME: This component definitely needs a refactor. Its right now very confusing
  how somethings are done. We should never expect an app to have props AND a store
  and modify both to update the component. The contract should be simple and straightforward

  Right now we reset the store and update store at random places completely outside
  the scope of the component. Eg: updateKeyValueStore from PipelineConfigurationsStore ActionCreator.

  This will warrant its own PR as it affects quite a number of places.
*/

import cloneDeep from 'lodash/cloneDeep';
import PropTypes from 'prop-types';
import React, { Component } from 'react';
import { connect, Provider } from 'react-redux';
import { objectQuery } from 'services/helpers';
import KeyValuePair from './KeyValuePair';
import KeyValueStore from './KeyValueStore';
import KeyValueStoreActions from './KeyValueStoreActions';

// Prop Name is used in place of the reserved prop 'key'
const mapStateToFieldNameProps = (state, ownProps) => {
  return {
    name: objectQuery(state.keyValues.pairs, ownProps.index, 'key'),
    value: objectQuery(state.keyValues.pairs, ownProps.index, 'value'),
    provided: objectQuery(state.keyValues.pairs, ownProps.index, 'provided'),
    notDeletable: objectQuery(state.keyValues.pairs, ownProps.index, 'notDeletable'),
    showReset: objectQuery(state.keyValues.pairs, ownProps.index, 'showReset'),
  };
};

const fieldToActionMap = {
  key: KeyValueStoreActions.setKey,
  value: KeyValueStoreActions.setVal,
};

const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    removeRow: () => {
      dispatch({
        type: KeyValueStoreActions.deletePair,
        payload: { index: ownProps.index },
      });
    },
    addRow: () => {
      dispatch({
        type: KeyValueStoreActions.addPair,
        payload: { index: ownProps.index },
      });
    },
    onChange: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          [fieldProp]: e.target.value,
        },
      });
    },
    onProvided: (e) => {
      dispatch({
        type: KeyValueStoreActions.setProvided,
        payload: {
          index: ownProps.index,
          provided: e.target.checked,
        },
      });
    },
  };
};

let KeyValuePairCopy = connect(mapStateToFieldNameProps, mapDispatchToFieldNameProps)(KeyValuePair);

export default class KeyValuePairs extends Component {
  constructor(props) {
    super(props);
    var { keyValues } = props;
    this.state = {
      pairs: cloneDeep(keyValues.pairs),
    };
    KeyValueStore.dispatch({
      type: KeyValueStoreActions.onUpdate,
      payload: { pairs: keyValues.pairs },
    });
  }

  componentDidMount() {
    var { onKeyValueChange } = this.props;
    this.subscription = KeyValueStore.subscribe(() => {
      this.setState(KeyValueStore.getState().keyValues);
      if (typeof onKeyValueChange === 'function') {
        onKeyValueChange(KeyValueStore.getState().keyValues);
      }
    });
  }

  componentWillUnmount() {
    this.subscription();
    KeyValueStore.dispatch({
      type: KeyValueStoreActions.onReset,
    });
  }
  componentWillReceiveProps(nextProps) {
    if (nextProps.keyValues.pairs.length !== this.state.pairs.length) {
      this.setState({
        pairs: [...nextProps.keyValues.pairs],
      });
    }
  }

  render() {
    return (
      <div>
        {this.state.pairs.map((pair, index) => {
          return (
            <div key={pair.uniqueId}>
              <Provider store={KeyValueStore}>
                <KeyValuePairCopy
                  index={index}
                  getResettedKeyValue={this.props.getResettedKeyValue}
                  keyPlaceholder={this.props.keyPlaceholder}
                  valuePlaceholder={this.props.valuePlaceholder}
                  disabled={this.props.disabled}
                  onPaste={this.props.onPaste}
                />
              </Provider>
            </div>
          );
        })}
      </div>
    );
  }
}

KeyValuePairs.propTypes = {
  keyValues: PropTypes.shape({
    pairs: PropTypes.arrayOf(
      PropTypes.shape({
        key: PropTypes.string,
        value: PropTypes.string,
        uniqueId: PropTypes.string,
        provided: PropTypes.bool,
        notDeletable: PropTypes.bool,
        showReset: PropTypes.bool,
      })
    ),
  }),
  onKeyValueChange: PropTypes.func,
  getResettedKeyValue: PropTypes.func,
  keyPlaceholder: PropTypes.string,
  valuePlaceholder: PropTypes.string,
  disabled: PropTypes.bool,
  onPaste: PropTypes.func,
};
