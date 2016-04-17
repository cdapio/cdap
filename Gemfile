source 'https://rubygems.org'

gem 'berkshelf', '~> 3.0'
gem 'foodcritic', '~> 4.0'

gem 'chefspec', '~> 4.0'
gem 'rspec', '~> 3.0'

if RUBY_VERSION.to_f < 2.0
  gem 'chef', '< 12.0'
  gem 'varia_model', '< 0.5.0'
else
  gem 'chef', '< 12.5' # Testing
end

gem 'chef-zero', '< 4.6' if RUBY_VERSION.to_f < 2.1

gem 'ridley', '~> 4.2.0'
gem 'faraday', '< 0.9.2'

gem 'rubocop'
gem 'rubocop-checkstyle_formatter', require: false
gem 'rainbow', '<= 1.99.1'

group :integration do
  gem 'test-kitchen'
  gem 'kitchen-vagrant'
end
