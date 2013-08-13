module NewsTagger
  module Retriever
    class S3Cache
      def initialize
        config = YAML.load_file(Rails.root.join 'config/aws-config.yml')[Rails.env]
        cache_config = config[:s3_cache]
        @bucket = cache_config[:bucket]
        @prefix = cache_config[:prefix]
        @region = cache_config[:region]
        @s3 = AWS::S3.new :access_key_id => config[:access_key_id], :secret_access_key => config[:secret_access_key], :region => cache_config[:region]
        @s3_bucket = @s3.buckets[@bucket]
      end

      def retrieve_from_cache topic, url
        s3_key = "#{@prefix}#{topic}/#{Digest::SHA2.hexdigest(url)}"
        s3_obj = @s3_bucket.objects[s3_key]
        return false unless s3_obj.exists?
        content = ''
        s3_obj.read do |chunk|
          content += chunk
        end
        yield content
        true
      end

      def send_to_cache(topic, url, content, document_type, metadata={})
        content_type = nil
        case document_type
          when :html
            content_type = 'text/html'
          when :json
            content_type = 'application/json'
        end
        s3_key = "#{@prefix}#{topic}/#{Digest::SHA2.hexdigest(url)}"
        s3_obj = @s3_bucket.objects[s3_key]
        s3_obj.write(content, {:content_type => content_type, :metadata => metadata})
      end
    end
  end
end