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
          cookies_header
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

        def process_paragraph(node)
          result = {
              :emails => [],
              :quotes => [],
              :links => []
          }
          case node.name
            when 'ul'
              result = []
              node.css('li').each do |li|
                result << process_paragraph(li)
                li.remove if validate_all_processed li, 'ul > li'
              end
              return result
            when 'a'
              if not node.has_attribute? 'href' and node.has_attribute? 'name'
                result = {:anchor => node.attr('name')}
                node.remove
                return result
              end
            when /h(\d+)/
              result[:head_level] = $~[1]
          end

          texts = []
          node.children.each do |element|
            if element.type == Nokogiri::XML::Node::TEXT_NODE
              texts << element.text.strip
              element.remove
              next
            end

            case element.name
              when 'p'
                result[:sub_paragraphs] ||= []
                result[:sub_paragraphs] << process_paragraph(element)
                element.remove if validate_all_processed element, 'p > p'
              when 'strong', 'em'
                texts << element.text.strip
                element.remove
              when 'br'
                texts << "\n"
                element.remove
              when 'a'
                texts << element.text.strip
                a_url = element.attr('href')
                case a_url
                  when /^mailto:(.*)/
                    result[:emails] << $~[1]
                    element.remove
                  when /^\/public\/quotes\/main\.html\?type=(?<type>\w+)&symbol=(?<symbol>[\w\.:-]+)$/
                    result[:quotes] << {
                        :type => $~[:type],
                        :symbol => $~[:symbol]
                    }
                    element.remove
                  else
                    # arbitrary link
                    result[:links] << a_url
                    element.remove
                end
              when 'cite'
                result[:cites] ||= []
                result[:cites] << process_paragraph(element)
                element.remove if validate_all_processed element, 'p > cite'
              when 'span'
                if (element.has_attribute? 'class' and element.attr('class').split(' ').include? 'quo') or
                    (element.has_attribute? 'data-widget')
                  element.remove
                  next
                end
                r = process_paragraph element
                if r.is_a? String
                  texts << r
                else
                  r.each do |k, v|
                    case k
                      when :text
                        texts << v
                      else
                        result[k] = result[k] + v
                    end
                  end
                end
                element.remove
            end
          end
          text = texts.join(' ').squeeze(' ')
          result.reject! { |k, v| v.empty? }
          return text if result.empty?
          result[:text] = text unless text.empty?
          result
        end

        def process_social_by_line_element(element)
          result = nil
          if element.text?
            result = element.text.strip
            element.remove
          elsif element.name == 'li'
            element.children.each do |li|
              unless result.nil?
                raise "Unsupported .socialByline Element:\n#{element.inspect}"
              end
              result = process_social_by_line_element li
            end
            element.remove if validate_all_processed element, '.socialByline li'
          end
          return result
        end

        def process_social_by_line(social_by_line)
          result = []
          social_by_line.css('li.connect').remove
          social_by_line.css('cite').each do |cite|
            cite.children.each do |element|
              r = process_social_by_line_element element
              result << r
            end
            cite.remove if validate_all_processed cite, '.socialByline > cite'
          end
          social_by_line.css('li.byName').each do |li|
            columnist = {}
            li.attributes.each do |name, value|
              case name
                when 'class'
                  next
                when 'data-dj-author-topicserviceid'
                  columnist[:author_topicserviceid] = li.attr(name) unless li.attr(name).nil? or li.attr(name).empty?
                else
                  raise "Unrecongized Attribute in li:\n#{li.inspect}"
              end
            end
            li.css('a').each do |a|
              url = a.attr('href')
              if url.start_with? 'http://topics.wsj.com/person'
                columnist[:url] = url
                columnist[:name] = a.text.strip
                a.remove
              end
            end
            li.children.each do |element|
              if element.text?
                columnist[:name] = element.text.strip
                element.remove
              end
            end
            result << columnist
            li.remove if validate_all_processed li, 'li.byName'
          end
          # text only by line
          social_by_line.css('li').each do |li|
            fw_sibling = li.next_sibling
            location = nil
            if fw_sibling and fw_sibling.text?
              case fw_sibling.text.strip
                when /in (.*) and/, /in (.*)/
                  location = $~[1]
              end
              fw_sibling.remove
            end
            if location.nil?
              result << li.text
            else
              result << {
                  :name => li.text,
                  :location => location
              }
            end
            li.remove
          end
          social_by_line.children.each do |element|
            if element.text?
              element.text.split("\n").each do |line|
                t = line.strip.downcase
                case t
                  when '', 'by', 'and'
                  when /by .*/
                    result << line.strip.match(/by (.*)/i)[1].strip
                  else
                    raise "Unrecognized .socialByline element:\n#{element.inspect}\n#{t.inspect}"
                end
              end
              element.remove
            end
          end
          return nil if result.empty?
          result
        end

        def process_head_comments(comment, metadata)
          article_start_flag = false
          comment_lines = comment.content.split("\n")
          if comment_lines.length > 1
            comment_lines.each do |comment_line|
              case comment_line.strip
                when /CODE=(\S*) SYMBOL=(\S*)/
                  metadata[:codes] ||= []
                  metadata[:codes] << {
                      :codes => $~[1],
                      :symbol => $~[2]
                  }
                #TODO: handle new type of comments
                else
                  raise("Unrecognized comment in .articleHeadlineBox:\n#{comment_line}")
              end
            end
          else
            comment_lines.each do |comment_line|
              comment_line.strip!
              case comment_line
                when /([^\s:]+):(.*)/
                  metadata[:properties] ||= []
                  metadata[:properties] << {
                      :key => $~[1],
                      :value => $~[2]
                  }
                when 'article start'
                  article_start_flag = true
                else
                  raise("Unrecognized comment in .articleHeadlineBox:\n#{comment_line}")
              end
            end
          end

          {
              :article_start_flag => article_start_flag
          }
        end


        def process_article(url, content)
          doc = Nokogiri::HTML(content)

          article = {
              :url => url,
          }
          article_start_flag = false
          article_end_flag = false


          parsed_metadata = {}
          article_headline_box = doc.css('.articleHeadlineBox').first
          if article_headline_box.nil?
            article[:_no_head] = true
          else
            metadata = article_headline_box.css('.cMetadata').first
            metadata.css('li.articleSection').each do |li|
              li.css('a').each do |a|
                parsed_metadata[:article_section] ||= []
                parsed_metadata[:article_section] << {
                    :url => a.attr('href'),
                    :name => a.text
                }
                a.remove
              end
              li.children.each do |element|
                if element.text?
                  parsed_metadata[:article_section] ||= []
                  parsed_metadata[:article_section] << element.text
                  element.remove
                end
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
              if element.comment?
                r = process_head_comments element, parsed_metadata
                article_start_flag = true if r[:article_start_flag]
                element.remove
              elsif element.name == 'a' and element.has_attribute?('name')
                parsed_metadata[:anchors] ||= []
                parsed_metadata[:anchors] << element.attr('name')
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
            article_headline_box.css('h5').each do |h|
              article[:other_heads] ||= []
              article[:other_heads] << {
                  :head => h.text.strip,
                  :level => /h(\d+)/.match(h.name)[1]
              }
              h.remove
            end
            article_headline_box.css('.columnist').each do |columnist|
              columnist.css('.columnistByline').each do |columnist_by_line|
                columnist_by_line.css('.socialByline').each do |social_by_line|
                  by = process_social_by_line social_by_line
                  article[:by] = by unless by.nil?
                  social_by_line.remove if validate_all_processed social_by_line, '.columnist .social_by_line'
                end
                columnist_by_line.remove if validate_all_processed columnist_by_line, '.columnist_by_line'
              end
              columnist.children.each do |element|
                if element.text?
                  element.remove if element.text.strip == '-'
                end
              end
              columnist.css('.icon').remove
              columnist.remove if validate_all_processed columnist, '.columnist'
            end
            article_headline_box.remove if validate_all_processed article_headline_box, '.articleHeadlineBox'
          end

          article_story_body = doc.css("#article_story_body").first
          if article_story_body.nil?
            article[:_no_body] = true
          else
            article_story_body.css(".articlePage").each do |article_page|
              article_page.css('ul.socialByline').each do |social_by_line|
                by = process_social_by_line social_by_line
                article[:by] = by unless by.nil?
                social_by_line.remove if validate_all_processed social_by_line, '.socialByline'
              end
              article_page.children.filter('p, h4, h5, h6, ul, a, blockquote').each do |node|
                paragraph = process_paragraph node
                article[:paragraphs] ||= []
                article[:paragraphs] << paragraph unless paragraph.nil?
                node.remove if validate_all_processed node, '.articlePage > p'
              end
              article_page.children.filter('cite').each do |cite|
                article[:cites] ||= []
                article[:cites] << cite.text
                cite.remove
              end
              article_page.css('.insetContent', '.insetCol3wide', '.insetCol6wide', '.legacyInset').remove
              article_page.children.each do |element|
                if element.comment?
                  if element.content.strip == 'article end'
                    article_end_flag = true
                    element.remove
                    next
                  end
                end
              end

              article_page.remove if validate_all_processed article_page, '.articlePage'
            end
          end

          raise "Improper article start/end flag: start(#{article_start_flag}), end(#{article_end_flag})" unless (article_start_flag and article_end_flag) or
              (article[:_no_head] and article[:_no_body])

          article
        end
      end
    end
  end
end
