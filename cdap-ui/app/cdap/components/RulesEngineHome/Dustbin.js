import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { DropTarget } from 'react-dnd';
import cloneDeep from 'lodash/cloneDeep';

const style = {
  height: '12rem',
  width: '12rem',
  marginRight: '1.5rem',
  marginBottom: '1.5rem',
  color: 'white',
  padding: '1rem',
  textAlign: 'center',
  fontSize: '1rem',
  lineHeight: 'normal',
  float: 'left'
};

const dropTarget = {
  drop() {
    return { name: 'Dustbin' };
  }
};

const connect = (connect, monitor) => {
  console.log(monitor);
  return cloneDeep({
    connectDropTarget: connect.dropTarget(),
    isOverTarget: monitor.isOver(),
    canDrop: monitor.canDrop()
  });
};
class Dustbin extends Component {
  componentWillReceiveProps(nextProps) {
    console.log('CRP: ', nextProps.isOver);
  }
  render() {
    console.log('Render in Dustbin');
    const { canDrop, isOverTarget, connectDropTarget } = this.props;
    const isActive = canDrop && isOverTarget;

    console.log('isOver', isOverTarget.toString());

    let backgroundColor = '#222';
    if (isActive) {
      backgroundColor = 'darkgreen';
    } else if (canDrop) {
      backgroundColor = 'darkkhaki';
    }

    return connectDropTarget(
      <div style={{ ...style, backgroundColor }}>
        {isActive ?
          'Release to drop' :
          'Drag a box here'
        }
      </div>
    );
  }
}

Dustbin.propTypes = {
  connectDropTarget: PropTypes.func.isRequired,
  isOverTarget: PropTypes.bool.isRequired,
  canDrop: PropTypes.bool.isRequired
};
export default DropTarget('box', dropTarget, connect)(Dustbin);
