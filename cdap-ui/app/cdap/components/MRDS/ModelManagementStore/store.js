import { combineReducers, createStore } from 'redux';
import MRDSActions from './actions.js';

const defaultAction = {
  type: '',
  payload: {}
};
const defaultState = {
  experiments: [],
};
const defaultInitialState = {
  mrdsState: defaultState,
};

const mrdsState = (state = defaultState, action = defaultAction) => {
  switch (action.type) {
    case MRDSActions.fetchExperiements:
      state = {
        ...state,
        experiments: action.payload
      };
      break;
  }
  console.log("mrds state =>", state);
  return state;
};


const MRDSStore = createStore(
  combineReducers({
    mrdsState,
  }),
  defaultInitialState
);

export default MRDSStore;
