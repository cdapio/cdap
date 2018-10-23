import React from 'react';
import { cleanup, render } from 'react-testing-library';
import Page500 from 'components/500';

afterEach(cleanup);

describe('Page500', () => {
  it('renders correctly', () => {
    const { container } = render(<Page500 />);
    expect(container.firstChild).toMatchSnapshot();
  });
});

