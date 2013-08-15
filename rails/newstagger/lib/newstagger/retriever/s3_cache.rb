require 'aws-sdk'

# Override to use Signature Version 4
module AWS
  class S3
    class Request
      include AWS::Core::Signature::Version4

      def add_authorization! (credentials)
        super credentials
      end

      def string_to_sign(datetime)
        super datetime
      end

      def service
        's3'
      end
    end
  end
end

module NewsTagger
  module Retriever
    class S3Cache
      def initialize
        config = YAML.load_file(Rails.root.join 'config/aws-config.yml')[Rails.env]
        cache_config = config[:s3_cache]
        @bucket = cache_config[:bucket]
        @prefix = cache_config[:prefix]
        @region = cache_config[:region]
        @s3 = AWS::S3.new :access_key_id => config[:access_key_id],
                          :secret_access_key => config[:secret_access_key],
                          :region => cache_config[:region],
                          :logger => nil
        @s3_client = @s3.client
      end

      def retrieve_from_cache(topic, url, cache_cutoff = nil, cache_cutoff_key = nil)
        s3_key = "#{@prefix}#{topic}/#{Digest::SHA2.hexdigest(url)}"
        begin
          response = @s3_client.get_object :bucket_name => @bucket, :key => s3_key
          content = response[:data]
          metadata = response[:meta].symbolize_keys
        rescue AWS::S3::Errors::NoSuchKey => e
          return false
        rescue Exception => e
          raise e
        end
        unless cache_cutoff.nil?
          return false if metadata[cache_cutoff_key].nil? or Time.parse(metadata[cache_cutoff_key]) < cache_cutoff
        end
        yield content
        true
      end

      def send_to_cache(topic, url, content, document_type, metadata={}, reduced_redundancy = true)
        content_type = nil
        case document_type
          when :html
            content_type = 'text/html'
          when :json
            content_type = 'application/json'
        end
        s3_key = "#{@prefix}#{topic}/#{Digest::SHA2.hexdigest(url)}"
        begin
          @s3_client.put_object :bucket_name => @bucket,
                                :key => s3_key,
                                :data => content,
                                :content_length => content.bytesize,
                                :storage_class => reduced_redundancy ? 'REDUCED_REDUNDANCY' : 'STANDARD',
                                :content_type => content_type,
                                :metadata => metadata
        rescue Exception => e
          raise e
        end
      end
    end
  end
end