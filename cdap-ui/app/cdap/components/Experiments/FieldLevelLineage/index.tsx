import React from 'react';
import './index.css';
import App from './App';
import { Provider } from './FllContext';

export default function FllExpt() {
  return (
    <Provider>
      <App />
    </Provider>
  );
}
