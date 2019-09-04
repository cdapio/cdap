import { FETCH_EXPERIEMENTS } from "./constants.js";
const initialState = {
  experiements: []
};
function rootReducer(state = initialState, action) {
  if (action.type === FETCH_EXPERIEMENTS) {
    return Object.assign({}, state, {
      experiements: state.experiements.concat(action.payload)
    });
  }
  return state;
}
export default rootReducer;
