/*
* Copyright © 2016-2017 Cask Data, Inc.
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

import React, {Component, PropTypes} from 'react';
import {connect , Provider} from 'react-redux';
import KeyValueStore from './KeyValueStore';
import KeyValueStoreActions from './KeyValueStoreActions';
import KeyValuePair from './KeyValuePair';

// Prop Name is used in place of the reserved prop 'key'
const mapStateToFieldNameProps = (state, ownProps) => {
  return {
    name: state.keyValues.pairs[ownProps.index].key,
    value: state.keyValues.pairs[ownProps.index].value,
    provided: state.keyValues.pairs[ownProps.index].provided,
    notDeletable: state.keyValues.pairs[ownProps.index].notDeletable,
    showReset: state.keyValues.pairs[ownProps.index].showReset
  };
};

const fieldToActionMap = {
  key: KeyValueStoreActions.setKey,
  value: KeyValueStoreActions.setVal
};

const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    removeRow: () => {
      dispatch({
        type: KeyValueStoreActions.deletePair,
        payload: {index: ownProps.index}
      });
    },
    addRow: () => {
      dispatch({
        type: KeyValueStoreActions.addPair,
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
    },
    onProvided: (e) => {
      dispatch({
        type: KeyValueStoreActions.setProvided,
        payload: {
          index: ownProps.index,
          provided: e.target.checked
        }
      });
    }
  };
};

let KeyValuePairCopy = connect(
  mapStateToFieldNameProps,
  mapDispatchToFieldNameProps
)(KeyValuePair);

export default class KeyValuePairs extends Component {
  constructor(props) {
    super(props);
    var { keyValues, onKeyValueChange } = props;
    this.state = {
        pairs: [...keyValues.pairs]
    };
    KeyValueStore.dispatch({
      type: KeyValueStoreActions.onUpdate,
      payload: {pairs: keyValues.pairs}
    });
    this.subscription = KeyValueStore.subscribe(() => {
      this.setState(KeyValueStore.getState().keyValues);
      onKeyValueChange(KeyValueStore.getState().keyValues);
    });
  }

  shouldComponentUpdate(nextProps) {
    return this.state.pairs.length !== nextProps.keyValues.pairs.length;
  }

  componentWillUnmount() {
    this.subscription();
    KeyValueStore.dispatch({
      type: KeyValueStoreActions.onReset
    });
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      pairs: [...nextProps.keyValues.pairs]
    });
  }

  render() {
    return (
      <div>
      {
        this.state.pairs.map( (pair, index) => {
          return (
            <div key={pair.uniqueId}>
              <Provider store={KeyValueStore}>
                {
                  this.props.getResettedKeyValue ?
                    (
                      <KeyValuePairCopy
                        index={index}
                        getResettedKeyValue={this.props.getResettedKeyValue}
                      />
                    )
                  : <KeyValuePairCopy index={index}/>
                }

              </Provider>
            </div>
          );
        })
      }
      </div>
    );
  }
}

KeyValuePairs.propTypes = {
  keyValues: PropTypes.shape({
    pairs: PropTypes.arrayOf(PropTypes.shape({
      key : PropTypes.string,
      value : PropTypes.string,
      uniqueId : PropTypes.string,
      provided: PropTypes.bool,
      notDeletable : PropTypes.bool,
      showReset : PropTypes.bool
    }))
  }),
  onKeyValueChange: PropTypes.func,
  getResettedKeyValue: PropTypes.func
};
