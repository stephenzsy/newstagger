require 'newstagger/retriever/retriever'
require 'cgi'

require 'aws-sdk'

module NewsTagger
  module Vendor
    module WSJ
      class Retriever < NewsTagger::Retriever::S3CachedRetriever

        WEBSITE_VERSION = '20130825'
        PROCESSOR_VERSION = '20130825'

        def initialize
          super 'wsj', WEBSITE_VERSION, PROCESSOR_VERSION

          config = YAML.load_file(Rails.root.join 'config/aws-config.yml')[Rails.env]
          state_table_config = config[:state_table]
          table_name = state_table_config[:table_name]
          region = state_table_config[:region]
          @dynamoDB = AWS::DynamoDB.new :access_key_id => config[:access_key_id],
                                        :secret_access_key => config[:secret_access_key],
                                        :region => region,
                                        :logger => nil

          response = @dynamoDB.client.get_item(
              :table_name => table_name,
              :key => {
                  :hash_key_element => {:s => "wsj_cookies"}
              })
          @cookies = response['Item']['value']['SS'];
        end

        def get_daily_index_url(local_date)
          "http://online.wsj.com/public/page/archive-#{local_date.strftime "%Y-%-m-%-d"}.html"
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
          archived_articles = doc.css('#archivedArticles')
          news_item = archived_articles.css('ul.newsItem')
          news_item.css('li').each do |item|
            a = item.css('a').first
            p = item.css('p').first
            url = a['href']
            a.remove
            result[:articles] << {
                :url => url,
                :title => a.text.strip,
                :summary => p.text
            }
          end
          result
        end

        def get_additional_headers_for_retrieve
          cookies_header = {"Cookie" => @cookies.join("; ")}
          p cookies_header
          cookies_header
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

        def validate_all_processed(e, name)
          e.children.each do |element|
            case element.type
              when Nokogiri::XML::Node::TEXT_NODE
                next if element.text.strip.empty?
            end
            raise "Unrecognized #{name} element:\n#{element.inspect()}"
          end
          true
        end

        def process_article(url, content)
          doc = Nokogiri::HTML(content)

          article = {
              :url => url,
          }
          article_start_flag = false
          article_end_flag = false

          begin
            parsed_metadata = {}
            article_headline_box = doc.css('.articleHeadlineBox').first
            metadata = article_headline_box.css('.cMetadata').first
            metadata.css('li.articleSection').each do |li|
              li.css('a').each do |a|
                parsed_metadata[:article_section] ||= []
                parsed_metadata[:article_section] << {
                    :section_url => a.attr('href'),
                    :section_name => a.text
                }
                a.remove
              end
              li.remove if li.children.empty?
            end
            metadata.css('li.dateStamp').each do |li|
              parsed_metadata[:date_stamp] = Time.parse(li.text()).strftime "%Y-%m-%d"
              li.remove
            end
            article[:metadata] = parsed_metadata
            metadata.remove if validate_all_processed metadata, '.cMetadata'
            article_headline_box.children.each do |element|
              case element.type
                when Nokogiri::XML::Node::COMMENT_NODE
                  element.content.strip.split("\n").each do |comment_line|
                    case comment_line
                      when /([^\s:]+):(.*)/
                        parsed_metadata[:properties] ||= []
                        parsed_metadata[:properties] << {
                            :key => $~[1],
                            :value => $~[2]
                        }
                      when /CODE=(\S*) SYMBOL=(\S*)/
                        parsed_metadata[:codes] ||= []
                        parsed_metadata[:codes] << {
                            :codes => $~[1],
                            :symbol => $~[2]
                        }
                      when 'article start'
                        article_start_flag = true
                      else
                        raise("Unrecognized comment in .articleHeadlineBox:\n#{comment_line}")
                    end
                  end
                  element.remove
              end
            end
            article_headline_box.css('h1').each do |h1|
              article[:title] = h1.text.strip
              h1.remove
            end
            article_headline_box.css('h2.subhead').each do |h2|
              article[:subtitle] = h2.text.strip
              h2.remove
            end
            article_headline_box.remove if validate_all_processed article_headline_box, '.articleHeadlineBox'
          end

          article_story_body = doc.css("#article_story_body").first
          article_story_body.css(".articlePage").each do |article_page|
            article_page.remove if validate_all_processed article_page, '.articlePage'
          end

          p article
          raise 'Need Developer'


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
