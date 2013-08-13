require 'newstagger/retriever/retriever'
require 'cgi'

module NewsTagger
  module Vendor
    module Bloomberg
      class Retriever < NewsTagger::Retriever::Retriever
        def initialize
          super 'bloomberg'
        end

        def get_daily_index_url date
          "http://www.bloomberg.com/archive/news/#{date.strftime "%Y-%m-%d"}/"
        end

        def process_index(content, date)
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
              when 'script'
                next
              when 'div'
                # TODO: related
                next
              when 'p'
                next if ['decoration-top', 'decoration'].include? node['class']
                t = ''
                current_paragraph = {}
                node.children.each do |n|
                  if n.type== Nokogiri::XML::Node::TEXT_NODE
                    t << n.text
                    next
                  end
                  case n.name
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
                    when 'span'
                      t << n.text
                    else
                      p n
                      raise 'unrecognized condition'
                  end
                end
                current_paragraph[:text] = t.strip.gsub("\n", ' ').squeeze(' ')
                current_section[:paragraphs] << current_paragraph
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
                next if node['class'] == 'decoration-bottom'
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
