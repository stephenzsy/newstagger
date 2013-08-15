require 'newstagger/retriever/s3_cache'

module NewsTagger
  module Retriever
    class Retriever

      def initialize(topic_vendor)
        @topic_vendor = topic_vendor
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

      def retrieve_daily_index(local_date, cache_cutoff_time = nil)
        yield Net::HTTP.get URI(get_daily_index_url(local_date))
        true
      end

      def retrieve_processed_daily_index(date, cache_cutoff_time = nil)
        retrieve_daily_index date, cache_cutoff_time do |content|
          yield process_daily_index content
        end
      end

      def retrieve_article(url)
        yield Net::HTTP.get(URI(url))
        true
      end

      def retrieve_processed_article(url)
        retrieve_article url do |content|
          yield process_article url, content
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
            retrieve_processed_article article[:url] do |normalized_article|
              yield normalized_article
            end
          end
        end
      end
    end

    class S3CachedRetriever < Retriever
      def initialize(topic_vendor, website_version, processor_version)
        super(topic_vendor)
        @cache = NewsTagger::Retriever::S3Cache.new
        @website_version = website_version
        @processor_version = processor_version
      end

      def retrieve_daily_index(local_date, cutoff_time)
        url = get_daily_index_url local_date
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:raw", url, cutoff_time, :retrieval_time) do |content, metadata={}|
          yield content
          return true
        end
        unless result
          super(local_date) do |content|
            @cache.send_to_cache "#{@topic_vendor}:daily_index:raw", url, content, :html, {
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
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:processed", url, cache_cutoff_time, :processed_time) do |content, metadata={}|
          yield JSON.parse content, :symbolize_names => true
        end
        unless result
          super local_date, cache_cutoff_time do |index|
            @cache.send_to_cache "#{@topic_vendor}:daily_index:processed", url, JSON.generate(index), :json, {
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

      def retrieve_article(url)
        result = @cache.retrieve_from_cache("#{@topic_vendor}:article:raw", url) do |content, metadata={}|
          yield content
          return true
        end
        unless result
          result = super(url) do |content|
            @cache.send_to_cache "#{@topic_vendor}:article:raw", url, content, :html, {
                :url => url,
                :retrieval_time => Time.now.utc.iso8601(3),
                :w_version => @website_version
            }, false
            yield content
          end
        end
        result
      end

      def retrieve_processed_article(url)
        result = @cache.retrieve_from_cache("#{@topic_vendor}:article:processed", url) do |content, metadata={}|
          yield JSON.parse content, :symbolize_names => true
        end
        unless result
          result = super(url) do |normalized_article|
            @cache.send_to_cache "#{@topic_vendor}:article:processed", url, JSON.generate(normalized_article), :json, {
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