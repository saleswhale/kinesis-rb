# frozen_string_literal: true

require_relative 'lib/kinesis/version'

Gem::Specification.new do |spec|
  spec.name          = 'kinesis'
  spec.version       = Kinesis::VERSION
  spec.authors       = ['Saleswhale']
  spec.email         = ['odina@saleswhale.com']

  spec.summary       = 'AWS Kinesis wrapper in Ruby'
  spec.homepage      = 'https://github.com/saleswhale/kinesis_rb'
  spec.license       = 'MIT'
  spec.required_ruby_version = Gem::Requirement.new('>= 2.4.0')

  spec.metadata['allowed_push_host'] = "we don't use a private gem server"

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = 'https://github.com/saleswhale/kinesis_rb.git'
  spec.metadata['changelog_uri'] = 'https://github.com/saleswhale/kinesis_rb.git'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake', '~> 13.0'
  spec.add_development_dependency 'rspec', '~> 3'
  spec.add_development_dependency 'rspec_junit_formatter'
  spec.add_development_dependency 'rubocop'

  spec.add_dependency 'aws-sdk-dynamodb', '~> 1'
  spec.add_dependency 'aws-sdk-kinesis', '~> 1'
  spec.add_dependency 'concurrent-ruby', '~> 1.0'
end
