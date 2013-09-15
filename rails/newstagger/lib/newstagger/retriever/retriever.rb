require 'newstagger/retriever/s3_cache'
require 'newstagger/retriever/credential_provider'

module NewsTagger
  module Retriever
    class Retriever

      def initialize(topic_vendor)
        @topic_vendor = topic_vendor
        @credential_provider = NewsTagger::AWSUtil::CredentialProvider.new
      end

      def get_daily_index_url(date)
        raise 'Not Supported'
      end

      def process_daily_index(content)
        raise 'Not Supported'
      end

      def process_article(url, content)
        raise 'Not Supported'
      end

      def get_additional_headers_for_retrieve()
        nil
      end

      def handle_set_cookie(set_cookie_line)
        nil
      end

      def filter_redirect_location(location)
        nil
      end

      def filter_response(response)
        case response.code
          when '200'
          when '302'
            handle_set_cookie response['set-cookie'] unless response['set-cookie'].nil?
            location = response['location']
            filter_redirect_location location
            return {:new_url => location}
          else
            p response
            raise "Fault Retrieve"
        end
        nil
      end

      def retrieve_daily_index(local_date, cache_cutoff_time = nil)
        uri = URI(get_daily_index_url(local_date))
        response = nil
        Net::HTTP.start(uri.host, uri.port) do |http|
          response = http.get(uri.path, get_additional_headers_for_retrieve())
        end
        filter_response response
        yield response.body();
        true
      end

      def retrieve_processed_daily_index(date, cache_cutoff_time = nil)
        retrieve_daily_index date, cache_cutoff_time do |content|
          yield process_daily_index content
        end
      end

      def retrieve_article(url, datetime)
        while true
          p url
          uri = URI(url)
          response = nil
          http = Net::HTTP.new(uri.host, uri.port)
          http.use_ssl = (uri.scheme == 'https')
          http.start do |http|
            response = http.get(uri.path, get_additional_headers_for_retrieve())
          end
          filter_result = filter_response response
          unless filter_result.nil?
            unless filter_result[:new_url].nil?
              url = filter_result[:new_url]
              next
            end
          end
          yield response.body();
          break
        end
        true
      end

      def retrieve_processed_article(url, datetime)
        retrieve_article url, datetime do |content|
          begin
            yield process_article url, content
          rescue
            puts "Error processing article URL: #{url}"
            raise
          end
        end
      end

      # get the local date of the index file
      def get_local_date(date)
        raise 'Not Supported'
      end

      def get_cache_cutoff_time(date)
        raise 'Not Supported'
      end

      def retrieve(date)
        local_date = get_local_date date
        cutoff_time = get_cache_cutoff_time date
        retrieve_processed_daily_index local_date, cutoff_time do |index|
          index[:articles].each do |article|
            retrieve_processed_article article[:url], local_date do |normalized_article|
              yield :normalized_article, normalized_article
            end
          end
        end
      end
    end

    class S3CachedRetriever < Retriever
      def initialize(topic_vendor, website_version, processor_version)
        super(topic_vendor)
        @cache = NewsTagger::Retriever::S3Cache.new @credential_provider
        @website_version = website_version
        @processor_version = processor_version
      end

      def retrieve_daily_index(local_date, cutoff_time)
        url = get_daily_index_url local_date
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:raw", local_date.strftime('%Y/%m/%d-'), url, cutoff_time, :retrieval_time) do |content, metadata={}|
          yield content
          return true
        end
        unless result
          super(local_date) do |content|
            @cache.send_to_cache "#{@topic_vendor}:daily_index:raw", local_date.strftime('%Y/%m/%d-'), url, content, :html, {
                :url => url,
                :local_date => local_date.iso8601,
                :retrieval_time => Time.now.utc.iso8601(3),
                :w_version => @website_version
            }, false
            yield content
          end
        end
        true
      end

      def retrieve_processed_daily_index(local_date, cache_cutoff_time)
        url = get_daily_index_url local_date
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:processed",
                                            local_date.strftime('%Y/%m/%d-'),
                                            url,
                                            cache_cutoff_time,
                                            :processed_time) do |content, metadata={}|
          yield JSON.parse content, :symbolize_names => true
        end
        unless result
          super local_date, cache_cutoff_time do |index|
            @cache.send_to_cache "#{@topic_vendor}:daily_index:processed", local_date.strftime('%Y/%m/%d-'), url, JSON.generate(index), :json, {
                :url => url,
                :local_date => local_date.iso8601,
                :processed_time => Time.now.utc.iso8601(3),
                :p_version => @processor_version
            }, true
            yield index
          end
        end
        result
      end

      def retrieve_article(url, datetime)
        result = @cache.retrieve_from_cache("#{@topic_vendor}:article:raw",
                                            datetime.strftime('%Y/%m/%d/'),
                                            url) do |content, metadata={}|
          yield content
          return true
        end
        unless result
          result = super(url, datetime) do |content|
            @cache.send_to_cache("#{@topic_vendor}:article:raw", datetime.strftime('%Y/%m/%d/'), url, content, :html, {
                :url => url,
                :retrieval_time => Time.now.utc.iso8601(3),
                :w_version => @website_version
            }, false) do |hash|

            end
            yield content
          end
        end
        result
      end

      def retrieve_processed_article(url, datetime)
        result = @cache.retrieve_from_cache("#{@topic_vendor}:article:processed", datetime.strftime('%Y/%m/%d/'), url) do |content, metadata={}|
          break (false) if metadata[:p_version] < @processor_version or metadata[:p_version].end_with? '-dev'
          yield JSON.parse content, :symbolize_names => true
          true
        end
        unless result
          result = super(url, datetime) do |normalized_article|
            @cache.send_to_cache "#{@topic_vendor}:article:processed", datetime.strftime('%Y/%m/%d/'), url, JSON.pretty_generate(normalized_article), :json, {
                :url => url,
                :processed_time => Time.now.utc.iso8601(3),
                :p_version => @processor_version
            }, true
            yield normalized_article
          end
        end
        result
      end
    end
  end
end