require 'net/http'
require 'nokogiri'
require 'digest/sha2'
require 'yaml'

require 'newstagger/retriever/s3_cache'
require 'newstagger/retriever/retriever'

module NewsTagger
  module Vendor
    module Reuters

      class Retriever < NewsTagger::Retriever::Retriever
        @@TOPIC_VENDOR = 'reuters'

        def initialize
          super 'reuters'
        end

        def get_daily_index_url date
          "http://www.reuters.com/resources/archive/us/#{date.strftime "%Y%m%d"}.html"
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
      end

    end
  end
end