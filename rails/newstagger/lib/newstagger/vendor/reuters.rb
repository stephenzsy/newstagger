require 'net/http'
require 'nokogiri'

module NewsTagger
  module Vendor
    module Reuters

      class Retriever
        def retrieve date
          # compose url
          uri = URI "http://www.reuters.com/resources/archive/us/#{date.strftime "%Y%m%d"}.html"
          content =  Net::HTTP.get uri

          doc = Nokogiri::HTML(content)
          doc.css('.primaryContent .headlineMed').each do |link|
            a = link.css('a').first
            article_url = a['href']
            article_title = a.children.first.text.strip
            timestamp = nil
            link.children.each do |node|
              timestamp = node.text if node.text?
            end
            timestamp = Time.parse(date.strftime "%Y-%m-%d #{timestamp}")
            puts "#{article_url}|#{article_title}|#{timestamp}"
          end
        end
      end

    end
  end
end