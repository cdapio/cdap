import React, {Component} from 'react';
import { DragDropContext } from 'react-dnd';
import HTML5Backend from 'react-dnd-html5-backend';
import Dustbin from './Dustbin';
import Box from './Box';


class Container extends Component {
  render() {
    console.log('Render in Container');
    return (
      <div>
        <div>
          <Box name="orange"/>
          <Box name="banana"/>
        </div>
        <Dustbin />
      </div>
    );
  }
}

export default DragDropContext(HTML5Backend)(Container);
