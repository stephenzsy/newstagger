require 'newstagger/retriever/s3_cache'

module NewsTagger
  module Retriever
    class Retriever

      def initialize(topic_vendor)
        @cache = NewsTagger::Retriever::S3Cache.new
        @topic_vendor = topic_vendor
      end

      def get_daily_index_url(date)
        raise 'Not Supported'
      end

      def retrieve_daily_index date
        url = get_daily_index_url date
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:raw", url) do |content, metadata={}|
          yield content
          return true
        end
        unless result
          uri = URI url
          content = Net::HTTP.get uri
          @cache.send_to_cache "#{@topic_vendor}:daily_index:raw", url, content, :html, {:url => url}
          yield content
        end
        true
      end

      def retrieve_processed_daily_index(date)
        url = get_daily_index_url date
        result = @cache.retrieve_from_cache("#{@topic_vendor}:daily_index:processed", url) do |content, metadata={}|
          yield JSON.parse content, :symbolize_names => true
        end
        unless result
          result = retrieve_daily_index date do |content|
            index = process_index content, date
            @cache.send_to_cache "#{@topic_vendor}:daily_index:processed", url, JSON.generate(index), :json, {:url => url}
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
          uri = URI url
          content = Net::HTTP.get uri
          @cache.send_to_cache "#{@topic_vendor}:article:raw", url, content, :html, {:url => url}
          yield content
        end
        true
      end

      def retrieve_processed_article(url)
        result = @cache.retrieve_from_cache("#{@topic_vendor}:article:processed", url) do |content, metadata={}|
          yield JSON.parse content, :symbolize_names => true
        end
        unless result
          result = retrieve_article url do |content|
            normalized_article = process_article url, content
            @cache.send_to_cache "#{@topic_vendor}:article:processed", url, JSON.generate(normalized_article), :json, {:url => url}
            yield normalized_article
          end
        end
        result
      end


      def retrieve(date)
        retrieve_processed_daily_index date do |index|
          index[:articles].each do |article|
            retrieve_processed_article article[:url] do |normalized_article|
              puts "#{article[:url]} : #{normalized_article[:by]}"
            end
          end
        end
      end
    end
  end
end