require 'net/http'
require 'nokogiri'
require 'digest/sha2'
require 'yaml'

module NewsTagger
  module Vendor
    module Reuters

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

      class Retriever
        @@TOPIC_VENDOR = 'reuters'

        def initialize
          @cache = S3Cache.new
        end

        def get_daily_index_url date
          "http://www.reuters.com/resources/archive/us/#{date.strftime "%Y%m%d"}.html"
        end

        def retrieve_daily_index date
          url = get_daily_index_url date
          result = @cache.retrieve_from_cache("#{@@TOPIC_VENDOR}:daily_index:raw", url) do |content, metadata={}|
            yield content
            return true
          end
          unless result
            uri = URI url
            content = Net::HTTP.get uri
            @cache.send_to_cache "#{@@TOPIC_VENDOR}:daily_index:raw", url, content, :html, {:url => url}
            yield content
          end
          true
        end

        def retrieve_article(url)
          result = @cache.retrieve_from_cache("#{@@TOPIC_VENDOR}:article:raw", url) do |content, metadata={}|
            yield content
            return true
          end
          unless result
            uri = URI url
            content = Net::HTTP.get uri
            @cache.send_to_cache "#{@@TOPIC_VENDOR}:article:raw", url, content, :html, {:url => url}
            yield content
          end
          true
        end

        def retrieve_processed_daily_index date
          url = get_daily_index_url date
          result = @cache.retrieve_from_cache("#{@@TOPIC_VENDOR}:daily_index:processed", url) do |content, metadata={}|
            yield JSON.parse content, :symbolize_names => true
          end
          unless result
            result = retrieve_daily_index date do |content|
              index = process_index content, date
              @cache.send_to_cache "#{@@TOPIC_VENDOR}:daily_index:processed", url, JSON.generate(index), :json, {:url => url}
              yield index
            end
          end
          result
        end

        def process_index(content, date)
          result = {
              :articles => [],
              :videos => []
          }
          doc = Nokogiri::HTML(content)
          doc.css('.primaryContent .headlineMed').each do |link|
            a = link.css('a').first
            article_url = a['href']
            category = :articles
            category = :videos if article_url.start_with? 'http://www.reuters.com/news/video'
            article_title = a.children.first.text.strip
            timestamp = nil
            link.children.each do |node|
              timestamp = node.text if node.text?
            end
            timestamp = Time.parse(date.strftime "%Y-%m-%d #{timestamp}")
            result[category] << {
                :url => article_url,
                :title => article_title,
                :timestamp => timestamp
            }
          end
          result
        end

        def process_paragraph(node)
          node.css('.articleLocation').remove
          text = node.text.strip.gsub("\n", ' ').squeeze(' ')
          {:text => text}
        end

        def process_article(url, content)
          doc = Nokogiri::HTML(content)

          article = {
              :url => url,
              :paragraphs => []
          }

          article_content = doc.css('#content #articleContent').first
          content_section = article_content.css(".sectionContent .sectionColumns .column2").first
          article[:heading] = content_section.css('h1').first.children.text.strip

          text_section = content_section.css("#articleText").first
          article_info = text_section.css("#articleInfo").first
          by_line = article_info.css(".byline").first
          unless by_line.nil?
            article[:by] = /^By (.*)$/.match(by_line.text.strip)[1]
          end
          location = article_info.css(".location").first
          unless location.nil?
            article[:location] = location.text.strip
          end
          timestamp = article_info.css(".timestamp").first
          unless timestamp.nil?
            article[:timestamp] = Time.parse(timestamp.text).utc.iso8601
          end

          focus_paragraph = process_paragraph text_section.css(".focusParagraph p").first
          focus_paragraph[:focus] = true
          article[:paragraphs] << focus_paragraph
          text_section.children.filter('p').each do |node|
            article[:paragraphs] << process_paragraph(node)
          end

          article
        end

        def retrieve date
          result = retrieve_processed_daily_index date do |index|
            index[:articles].each do |article|
              retrieve_article article[:url] do |content|
                normalized_article = process_article article[:url], content
                puts JSON.pretty_generate normalized_article
                return

              end
            end
          end
          result
        end
      end

    end
  end
end