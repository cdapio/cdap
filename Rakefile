#!/usr/bin/env rake

require 'bundler/setup'

# chefspec task against spec/*_spec.rb
require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:chefspec)

# foodcritic rake task
desc 'Foodcritic linter'
task :foodcritic do
  sh 'foodcritic -f correctness .'
end

# rubocop rake task
desc 'Ruby style guide linter'
task :rubocop do
  sh 'rubocop -D'
end

# creates metadata.json
desc 'Create metadata.json from metadata.rb'
task :metadata do
  sh 'knife cookbook metadata from file metadata.rb'
end

# share cookbook to Chef community site
desc 'Share cookbook to community site'
task :share do
  sh 'knife cookbook site share cdap databases'
end

# run vagrant test
desc 'Run vagrant tests'
task :vagrant do
  sh 'vagrant up'
end

# test-kitchen
begin
  require 'kitchen/rake_tasks'
  desc 'Run Test Kitchen integration tests'
  task :integration do
    Kitchen.logger = Kitchen.default_file_logger
    Kitchen::Config.new.instances.each do |instance|
      instance.test(:always)
    end
  end
rescue LoadError
  puts '>>>>> Kitchen gem not loaded, omitting tasks' unless ENV['CI']
end

# default tasks are quick, commit tests
task :default => %w(foodcritic rubocop chefspec)
