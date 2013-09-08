require 'newstagger/retriever/retriever'
require 'cgi'

require 'aws-sdk'

module NewsTagger
  module Vendor
    module WSJ
      class Retriever < NewsTagger::Retriever::S3CachedRetriever

        WEBSITE_VERSION = '20130825'
        PROCESSOR_VERSION = '20130825'
        PROCESSOR_PATCH = 2
        TIME_ZONE = ActiveSupport::TimeZone['America/New_York']

        def initialize(opt={})
          @test_mode = opt[:test_mode]
          @test_mode = false if @test_mode.nil?
          super 'wsj', WEBSITE_VERSION, PROCESSOR_VERSION

          config = YAML.load_file(Rails.root.join 'config/aws-config.yml')[Rails.env]
          region = config[:region]

          @dynamoDB = AWS::DynamoDB.new :credential_provider => @credential_provider,
                                        :region => region,
                                        :logger => nil
          @sns = AWS::SNS.new :credential_provider => @credential_provider,
                              :region => region
          @sns_notification_topic = config[:sns_notification_topic]

          state_table_config = config[:state_table]
          @state_table_name = state_table_config[:table_name]

          @state_table = @dynamoDB.tables[@state_table_name]
          @state_table.load_schema

          @error_table = @dynamoDB.tables[config[:error_table][:table_name]]
          @error_table.load_schema

          response = @dynamoDB.client.get_item(
              :table_name => @state_table_name,
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

        def filter_redirect_location(location)
          super(location)
          raise 'Invalid location' unless URI.parse(location).host().end_with? 'wsj.com'
        end

        def handle_set_cookie(set_cookie_line)
          cookies = {}
          set_cookie_line.split(/,\s*/).each do |cookie_line|
            if cookie_line.match /^(?<name>djcs_\w+)=(?<value>[^;]*)/
              cookies[$~[:name]] = $~[:value]
            elsif cookie_line.match /^user_type=subscribed/
              cookies['user_type'] = 'subscribed'
            end
          end
          if (cookies['djcs_auto'] and cookies['djcs_perm'] and cookies['djcs_session'] and cookies['user_type'])
            @cookies = [
                "djcs_auto=#{cookies['djcs_auto']}",
                "djcs_perm=#{cookies['djcs_perm']}",
                "djcs_session=#{cookies['djcs_session']}",
                "user_type=#{cookies['user_type']}"
            ]
            @dynamoDB.client.put_item(
                :table_name => @state_table_name,
                :item => {
                    'key' => {:s => "wsj_cookies"},
                    'value' => {
                        :ss => @cookies
                    }
                },
                :return_values => 'NONE'
            )
          end
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
          if node.has_attribute? 'class'
            classes = node.attr('class').split(' ')
            if classes.include? 'articleVersion'
              result[:tags] ||= []
              result[:tags] << 'articleVersion'
            end
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
              when 'span', 'no'
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

        def process_social_by_line (social_by_line)

          def handle_by_li(li)
            result = {}
            while true
              fw_sibling = li.next_sibling
              if fw_sibling and fw_sibling.text? or
                  (not fw_sibling.nil? and fw_sibling.name == 'strong')
                case fw_sibling.text.strip
                  when '', '|'
                    fw_sibling.remove
                    next
                  when /in (.*) and/, /in (.*),?/
                    result[:location] = $~[1].strip
                    fw_sibling.remove
                  when /from (.*)/i
                    result[:organization] = $~[1].strip
                    fw_sibling.remove
                end
              end
              break
            end

            author = nil

            if li.has_attribute? 'class' and li.attr("class").split(" ").include? 'byName'
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
              author = columnist
            else
              li.children.each do |element|
                if element.text?
                  author = element.text.strip
                  element.remove
                end
              end
            end

            return author if result.empty?
            result[:name] = author
            result
          end

          result = []
          social_by_line.css('li.connect').remove
          social_by_line.css('cite').each do |cite|
            cite.children.each do |element|
              r = process_social_by_line_element element
              result << r
            end
            cite.remove if validate_all_processed cite, '.socialByline > cite'
          end
          social_by_line.css('li').each do |li|
            result << handle_by_li(li)
            li.remove if validate_all_processed li, 'li'
          end
          social_by_line.children.each do |element|
            if element.text?
              line = element.text.gsub("\n", ' ').squeeze(' ').strip
              t = line.downcase
              case t
                when '', /by\s*/, /and\s*/, ','
                when 'de'
                  # foreign language TODO handle
                when /by .*/
                  result << line.strip.match(/by (.*)/i)[1].strip
                else
                  raise "Unrecognized .socialByline element:\n#{element.inspect}\n#{t.inspect}"
              end
              element.remove
            end
          end
          return nil if result.empty?
          result
        end

        def handle_indented(level, lines)
          result = []
          until lines.empty?
            line = lines.first
            m = /^(\s*)(\S.*)?/.match(line)
            indent_length = m[1].size
            line = m[2]
            if indent_length > level
              result << handle_indented(indent_length, lines)
            elsif indent_length == level
              unless line.nil? or line.empty?
                result << line
              end
              lines.shift
            else
              break
            end
          end
          result.reject! { |e| e.nil? or e.empty? }
          result
        end

        def process_head_comments(comment, metadata)
          article_start_flag = false
          comment_lines = comment.content.split("\n")
          if comment_lines.length > 1
            # multiline comment
            until comment_lines.empty?
              line = comment_lines.first
              if line.empty?
                comment_lines.shift
                next
              end

              if line.match /^CODE=(\S*) SYMBOL=(\S*)/
                metadata[:codes] ||= []
                metadata[:codes] << {
                    :code => $~[1],
                    :symbol => $~[2]
                }
                comment_lines.shift
                next
              end

              metadata[:other] ||= []
              metadata[:other] << handle_indented(0, comment_lines)

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
            article_headline_box.css('.cMetadata').each do |metadata|
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
            end

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
              fix_article_page! article_page
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
              article_page.css('.insetContent', '.insetCol3wide', '.insetCol6wide', '.embedType-interactive').remove
              article_page.children.each do |element|
                if element.comment?
                  if element.content.strip == 'article end'
                    article_end_flag = true
                    element.remove
                    next
                  end
                end
                if element.name == 'div'
                  element.css('.offDutyMoreSection').remove
                  element.remove if validate_all_processed element, '.articlePage div'
                  next
                end
              end

              article_page.remove if validate_all_processed article_page, '.articlePage'
            end
          end

          raise "Improper article start/end flag: start(#{article_start_flag}), end(#{article_end_flag})" unless (article_start_flag and article_end_flag) or
              (article[:_no_head] and article[:_no_body])

          article
        end

        def fix_article_page!(article_page)
          article_page.css('.legacyInset').each do |legacy_insert|
            legacy_insert.css('.insetContent').remove
            legacy_insert.before(legacy_insert.children)
            legacy_insert.remove if validate_all_processed legacy_insert, '.legacyInset'
          end
        end

        def retrieve(date = nil)
          logger = Rails.logger
          if date.nil?
            # auto determine the date to be retrieved from the database
            last_processed_date_item = @state_table.items.at("wsj-last_processed_date-#{PROCESSOR_VERSION}")
            if last_processed_date_item.exists?
              date = Time.parse(last_processed_date_item.attributes['value']).in_time_zone(TIME_ZONE) + 1.day
            else
              date = TIME_ZONE.parse('2009-04-01')
            end
            if date > Time.now
              date = Time.now
            end

          end
          local_date = get_local_date date
          @state_table.items.create({'key' => "wsj-last_processed_date-#{PROCESSOR_VERSION}",
                                     'value' => local_date.utc.iso8601}) unless @test_mode
          begin


            logger.info "Begin process WSJ on date: #{local_date}"
            yield :date, local_date
            super(local_date)
            logger.info "Complete process WSJ on date: #{local_date}"
          rescue Exception => e
            if (@test_mode)
              raise e
            end
            logger.error "Failed to process wsj on date #{local_date}"
            logger.error e.message
            logger.error e.backtrace.join("\n")
            @error_table.items.create('topic' => 'wsj-error',
                                      'date' => local_date.utc.iso8601,
                                      'processor_version' => PROCESSOR_VERSION,
                                      'processor_patch' => PROCESSOR_PATCH,
                                      'logged_at' => Time.now.utc.iso8601)
            @sns.topics[@sns_notification_topic].publish(([
                'Error in Execution',
                'Topic: wsj-error',
                "Date of Error: #{local_date.iso8601}",
                "Processor Version: #{PROCESSOR_VERSION}",
                "Processor Patch: #{PROCESSOR_PATCH}",
                "Time of Execution: #{Time.now.utc.iso8601}",
                "Error Message: #{e.message}",
                'Stack Trace:',
            ] + e.backtrace).join("\n"))
          end
        end

        def cleanup_status
          @error_table.items.each do |item|
            next unless item.hash_value == 'wsj-error'
            next unless item.attributes['processor_version'] == PROCESSOR_VERSION
            if item.attributes['processor_patch'].nil? or item.attributes['processor_patch'] < PROCESSOR_PATCH
              count = 0
              date = Time.parse(item.attributes['date'])
              retrieve date do |type, document|
                count += 1
                if type == :normalized_article
                  puts "R #{date}|#{count}: #{document[:url]}"
                end
              end
              item.attributes.add({'fix_patch' => PROCESSOR_PATCH}, {:if => {'processor_patch' => PROCESSOR_PATCH}})
            end
          end
        end
      end
    end
  end
end
