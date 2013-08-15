require 'newstagger/retriever/retriever'
require 'cgi'

module NewsTagger
  module Vendor
    module Bloomberg
      class Retriever < NewsTagger::Retriever::S3CachedRetriever

        WEBSITE_VERSION = '20130814'
        PROCESSOR_VERSION = '20130814'

        def initialize
          super 'bloomberg', WEBSITE_VERSION, PROCESSOR_VERSION
        end

        def get_daily_index_url(local_date)
          "http://www.bloomberg.com/archive/news/#{local_date.strftime "%Y-%m-%d"}/"
        end

        def get_local_date(date)
          date.in_time_zone('America/New_York').midnight
        end

        def get_cache_cutoff_time(date)
          get_local_date(date) + 1.day + 15.minutes
        end

        def process_daily_index(content)
          result = {
              :articles => [],
          }
          doc = Nokogiri::HTML(content)
          stories = doc.css('body #content .stories').first
          return result if stories.nil?
          stories.css('li').each do |item|
            a = item.css('a').first
            url = "http://www.bloomberg.com#{a['href']}"
            a.remove
            result[:articles] << {
                :url => url,
                :title => a.text.strip
            }
          end
          result
        end

        def parse_html_text_block(node, article)
          return nil if ['decoration-top', 'decoration'].include? node['class']
          t = ''
          current_paragraph = {}
          node.children.each do |n|
            if n.type== Nokogiri::XML::Node::TEXT_NODE
              t << n.text
              next
            end
            case n.name
              when 'p'
                current_paragraph[:sub_paragraphs] ||= []
                current_paragraph[:sub_paragraphs] << parse_html_text_block(n, article)
              when 'a'
                if n['class'] == 'web_ticker'
                  t << n.text
                  current_paragraph[:tickers] ||= []
                  if n['ticker'].nil?
                    current_paragraph[:tickers] << /^\/quote\/(.*)$/.match(n['href'])[1]
                  else
                    current_paragraph[:tickers] << n['ticker']
                  end
                elsif n['href'].start_with? 'http://topics.bloomberg.com/'
                  t << n.text
                  current_paragraph[:topics] ||= []
                  current_paragraph[:topics] << URI.parse(n['href']).path.gsub(/^\//, '').gsub(/\/$/, '')
                elsif n['href'].start_with? 'http://search.bloomberg.com/search'
                  t << n.text
                  current_paragraph[:searches] ||= []
                  current_paragraph[:searches] << CGI.parse(URI.parse(n['href']).query)['q']
                elsif n['href'].start_with? 'mailto:'
                  t << n.text
                  article[:emails] ||= []
                  article[:emails] << n['href'].gsub(/^mailto:/, '')
                else
                  t << n.text
                  current_paragraph[:links] ||= []
                  current_paragraph[:links] << n['href']
                end
              when 'span', 'b', 'strong', 'em'
                t << n.text
              when 'br'
                t << ' '
              when 'i', 'img'
                next t << ' '
              else
                p n
                raise 'unrecognized condition'
            end
          end
          current_paragraph[:text] = t.strip.gsub("\n", ' ').squeeze(' ')
          current_paragraph
        end

        def process_article(url, content)
          doc = Nokogiri::HTML(content)

          article = {
              :url => url,
              :sections => [],
          }

          primary_content = doc.css('#content #primary_content').first
          story_head = primary_content.css('#story_head').first
          article[:title] = story_head.css('h1').text.strip
          story_meta = story_head.css('#story_meta').first
          if story_meta.nil?
            story_meta = story_head.css('.bview_story_meta').first if story_meta.nil?
            article[:by] ||= []
            story_meta.css('.author').each do |element|
              article[:by] << element.text.strip
            end
            story_meta.css('span').each do |element|
              article[:by] << element.text.strip if element['class'].nil? or element['class'] == 'last'
            end
          else
            byline = story_meta.css('.byline').first
            article[:by] ||= []
            byline.css('span').each do |element|
              article[:by] << element.text.strip if element['class'].nil? or element['class'] == 'last'
            end
          end
          article[:timestamp] = Time.at(story_meta.css('.datestamp').first['epoch'].to_i / 1000).utc.iso8601
          story_display = primary_content.css("#story_content #story_display").first
          current_section = {
              :title => '',
              :paragraphs => []
          }
          has_content = false
          story_display.children.each do |node|
            next if node.type == Nokogiri::XML::Node::TEXT_NODE
            case node.name
              when 'script',
                  'br',
                  'i', 'img'
                next
              when 'div'
                # TODO: related
                next
              when 'ol', 'ul'
                list = []
                node.css('li').each do |element|
                  list << parse_html_text_block(element, article)
                end
                current_section[:paragraphs] << list
                has_content = true
              when 'blockquote', 'em'
                node.css('p').each do |n|
                  current_section[:paragraphs] << parse_html_text_block(n, article)
                  has_content = true
                end
              when 'p', 'b'
                current_section[:paragraphs] << parse_html_text_block(node, article)
                has_content = true
              when 'h2'
                article[:sections] << current_section
                current_section = {
                    :title => node.text.strip,
                    :paragraphs => []
                }
                has_content = true
              when 'pre'
                current_section[:paragraphs] << {
                    :text => node.text
                }
              else
                p node if node['class'].nil?
                next if node['class'] == 'decoration-bottom' or node['class'].include? 'mt-enclosure'
                p node
                raise 'unrecognized condition'
            end
          end
          article[:sections] << current_section if has_content
          article
        end
      end
    end
  end
end
