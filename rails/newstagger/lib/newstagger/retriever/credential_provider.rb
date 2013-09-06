require 'aws-sdk'

module NewsTagger
  module AWSUtil
    class CredentialProvider
      def on_ec2_instance?
        output = `ec2-metadata 2>&1`
        return true if $?.exitstatus == 0
        false
      end

      def initialize
        if on_ec2_instance?
          @inner_provider = AWS::Core::CredentialProviders::EC2Provider.new
        else
          @inner_provider = AWS::Core::CredentialProviders::CredentialFileProvider.new Rails.root.join('config', 'aws-dev-creds')
        end
      end

      def method_missing(name, *args)
        @inner_provider.send(name, *args)
      end
    end
  end
end