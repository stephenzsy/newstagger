require 'newstagger/retriever/retriever'
require 'cgi'
require 'newstagger/retriever/parser'

require 'aws-sdk'

module NewsTagger
  module Vendor
    module WSJ
      class Retriever < NewsTagger::Retriever::S3CachedRetriever

        WEBSITE_VERSION = '20130825'
        PROCESSOR_VERSION = '2013091504'
        PROCESSOR_PATCH = 7
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

        module Parsers
          include NewsTagger::Parsers

          class HeadMetaParser < HTMLParser

            @@HEAD_META_NAME_BLACKLIST = Set.new(
                [
                    'msapplication-task',
                    'format-detection',
                    "apple-itunes-app",
                    "application-name",
                    'sitedomain',
                    'primaryproduct',
                    'GOOGLEBOT'
                ])

            def parse(node)
              return nil unless node.has_attribute? 'content'
              type = nil
              key = nil
              if node.has_attribute? 'name'
                type = :name
                key = node.attr('name')
              elsif node.has_attribute? 'property'
                type = :property
                key = node.attr('property')
              else
                return nil
              end
              case key
                when /^(fb|twitter):/
                  return nil
              end
              return nil if @@HEAD_META_NAME_BLACKLIST.include? key
              {type => key, :value => node.attr('content')}
            end
          end # class HeadMetaParser

          class SocialBylineParser < HTMLParser
            include NewsTagger::Parsers::ParserRules

            class TextRule < RuleBase
              def parse(node_seq, parent_node)
                r = []
                raise ParserRuleNotMatchException unless node_seq.size == 1 and node_seq.first.text?
                text = node_seq.first.content.strip
                case text
                  when /^By ([[[:upper:]]\. ]+)( and ([[[:upper:]]\. ]+))?$/
                    r << {:author => [:name => $~[1]]}
                    r << {:author => [:name => $~[3]]} unless $~[3].nil?
                  else
                    raise ParserRuleNotMatchException
                end
                node_seq.unlink
                r
              end
            end # class TextRule

            class By_name_cite_Rule < RuleBase
              def parse(node_seq, parent_node)
                state = :S
                r = {}
                node_seq.each do |node|
                  case state
                    when :S
                      if node.text? and node.content.strip =~ /by (.*)/i
                        r[:author] = $~[1]
                        state = :name
                        next
                      end
                      raise ParserRuleNotMatchException
                    when :name
                      if node.name == 'cite' and node.children.size == 1 and node.children.first.text?
                        r[:cite] = node.children.first.content.strip
                        state = :F
                        next
                      end
                      raise ParserRuleNotMatchException
                  end
                end
                raise ParserRuleNotMatchException unless state == :F
                node_seq.unlink
                [r]
              end
            end # class By_name_cite_Rule

            class By_byName_star_Cite_Rule < RuleBase

              def parse(node_seq, parent_node)
                raise ParserRuleNotMatchException unless parent_node.element? and parent_node.name == 'ul'
                r = []
                state = :S
                loc_last_authors = []
                last_author = nil
                node_seq.each do |node|
                  case state
                    when :S
                      if node.text? and node.content.strip.downcase == 'by'
                        state = :by
                        next
                      elsif node.element? and ['li', 'cite'].include? node.name
                        state = :li
                        next
                      end
                    when :by, :li_separator
                      if node.element? and ['li', 'cite'].include? node.name
                        last_author = author = {:author => parse_li(node)}
                        loc_last_authors << author
                        r << author
                        state = :li
                        next
                      end
                    when :li_continue
                      if node.element? and node.name == 'li'
                        text = parse_li(node).each do |l|
                          break l[:name] if l.has_key? :name
                        end
                        last_author[:author].each do |l|
                          if l.has_key? :name
                            l[:name] += " #{text}"
                            break
                          end
                        end
                        state = :li
                        next
                      end
                    when :li
                      if node.element? and ['li', 'cite'].include? node.name
                        last_author = author = {:author => parse_li(node)}
                        loc_last_authors << author
                        r << author
                        state = :li
                        next
                      elsif node.text?
                        text = node.content.strip
                        if text == '|'
                          state = :cite_separator
                        elsif text == 'DE'
                          # foreign name
                          last_author[:author].each do |l|
                            if l.has_key? :name
                              l[:name] += " #{text}"
                              break
                            end
                          end
                          state = :li_continue
                        elsif text.match /^(in|at) (.*)( and|,)$/
                          location = $~[1]
                          loc_last_authors.each do |author|
                            author[:location] = location
                          end
                          loc_last_authors.clear
                          state = :li_separator
                        elsif text.match /^(in|at) (.*)$/
                          location = $~[1]
                          loc_last_authors.each do |author|
                            author[:location] = location
                          end
                          loc_last_authors.clear
                          state = :F
                        elsif ['and', ','].include? text.downcase
                          state = :li_separator
                        else
                          raise ParserRuleNotMatchException
                        end
                        next
                      end
                    when :F
                      if node.text?
                        text = node.content.strip
                        if text == '|'
                          state = :cite_separator
                          next
                        end
                      end
                    when :cite_separator
                      if node.name == 'cite'
                        text = node.text.strip
                        r << {:cite => text}
                        state = :F
                        next
                      end
                  end
                  raise ParserRuleNotMatchException
                end
                case state
                  when :li, :F
                  else
                    raise ParserRuleNotMatchException
                end
                r.reject! { |e| e.nil? }
                node_seq.unlink
                return nil if r.empty?
                r
              end

              def parse_li(node)
                node = node.dup
                r = []
                node.attributes.each do |name, attr|
                  if name.match /data-(\S+)/
                    r << {:data => {:name => $~[1], :value => attr.content}}
                    attr.unlink
                  end
                end
                name_parsed = false
                node.children.each do |nn|
                  if nn.text? and nn.content.strip.empty?
                    nn.unlink
                    next
                  end
                  raise 'Unsupported multiple entries in .socialByline > li' if name_parsed
                  if not node.attr('class').nil? and node.attr('class').split(/\s+/).include? 'byName' and nn.name == 'a'
                    r << {:link => nn.attr('href')}
                    nnn = nn.children.first
                    if nnn.text?
                      r << {:name => nnn.content.strip}
                      nnn.unlink
                    end
                  elsif nn.text?
                    r << {:name => nn.content.strip}
                    nn.unlink
                  end
                  name_parsed = true
                end

                ensure_empty_node node
                r.reject! { |x| x.nil? }
                return r
              end
            end # class By_byName_star_Rule

            @@node_sequence_rules = [
                By_byName_star_Cite_Rule.new,
                By_name_cite_Rule.new,
                TextRule.new,
            ]

            def parse(node)
              node.css('#connectButton').unlink
              node.css('li.connect').unlink
              node.children.each do |n|
                n.unlink if n.text? and n.content.strip.empty?
              end
              node_seq = node.children
              matched = false
              begin
                @@node_sequence_rules.each do |rule|
                  begin
                    r = rule.parse node_seq, node
                    ensure_empty_node node
                    return nil if r.nil?
                    return {:social_byline => r}
                  rescue ParserRuleNotMatchException
                    next
                  end
                end
                return {:social_by_line => super(node)}
              ensure
                node_seq.unlink if matched
              end
            end
          end

          class ArticleHeadlineBoxParser < HTMLParser
            @@social_byline_parser = SocialBylineParser.new

            # @return [Array]
            def parse(node)
              r = []

              # .cMetadata
              c_metadata = []
              select_only_node_to_parse(node, 'ul.cMetadata', true) do |c_metadata_node|
                c_metadata << select_only_node_to_parse(c_metadata_node, 'li.articleSection', true) do |article_section_node|
                  r_article_section = {:name => article_section_node.text.strip}
                  select_only_node_to_parse article_section_node, 'a', true do |a|
                    r_article_section[:link] = a.attr('href')
                  end
                  {:article_section => r_article_section}
                end
                c_metadata << select_only_node_to_parse(c_metadata_node, '.dateStamp') do |date_stamp|
                  text = date_stamp.text
                  date = Time.parse(text)
                  {:date_stamp => {:text => text, :date_stamp => date.iso8601}}
                end
                ensure_empty_node c_metadata_node
              end

              node.children.each do |n|
                if n.comment?
                  lines = n.content.split "\n"
                  if lines.size == 1
                    parsed_comment = parse_single_line_comment(n.content) do |state|
                      yield state
                    end
                    c_metadata << parsed_comment unless parsed_comment.nil?
                  else
                    c_metadata += parse_multi_line_comment(lines)
                  end
                  n.unlink
                elsif n.name == 'h5'
                  n.children.each do |nn|
                    nn.unlink if nn.text? and nn.content.strip.empty?
                  end
                  if n.children.size == 1
                    nn = n.children.first
                    if nn.text?
                      r << {:heading => n.text, :level => 5}
                      n.unlink
                    elsif nn.element? and nn.name == 'a' and nn.one_level_text?
                      r << {:heading => nn.text, :level => 5, :link => nn.attr('href')}
                      n.unlink
                    end
                  end
                end
              end
              r << {:c_metadata => c_metadata} unless c_metadata.empty?

              select_only_node_to_parse(node, 'h1') do |h1|
                r << {:headline => h1.text.strip}
              end
              select_only_node_to_parse(node, 'h2.subhead', true) do |h2|
                r << {:subhead => h2.text.strip}
              end
              select_only_node_to_parse node, '.columnist', true do |columnist_node|
                columnist_node.css('div.icon').unlink
                select_only_node_to_parse columnist_node, '.socialByline' do |social_byline_node|
                  r << @@social_byline_parser.parse(social_byline_node)
                end
                columnist_node.children.each do |node|
                  node.unlink if node.text? and node.text.strip == '-'
                end
                ensure_empty_node columnist_node
              end

              ensure_empty_node node
              r
            end

            private
            def parse_single_line_comment(line)
              line.strip!
              case line
                when /([^\s:]+):(.*)/
                  return {:key_value => {:key => $~[1], :value => $~[2]}}
                when 'article start'
                  yield ({:article_start_flag => true})
                else
                  raise("Unrecognized comment in .articleHeadlineBox:\n#{line}")
              end
              nil
            end

            def parse_multi_line_comment(lines)
              lines.shift if lines.first.empty?
              lines.pop if lines.last.empty?
              r = []
              until lines.empty? do
                line = lines.first
                if line.match /^CODE=(\S*) SYMBOL=(\S*)/
                  r << {:code_symbol => {:code => $~[1], :symbol => $~[2]}}
                  lines.shift
                  next
                end
                r << {:tree => handle_indented(0, lines)}
              end
              r
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

          end # class ArticleHeadlineBoxParser

          class ArticlePageParser < HTMLParser
            @@social_byline_parser = SocialBylineParser.new

            def parse(article_page_node)
              article_page_node.css('.insetContent', '.insetCol3wide', 'insetCol6wide', '.offDutyMoreSection').unlink

              # .socialByLine
              r = []
              social_byline = select_only_node_to_parse article_page_node, '.socialByline', true do |node|
                @@social_byline_parser.parse node
              end
              r << social_byline unless social_byline.nil?

              # paragraphs
              paragraphs = []
              f = []
              begin
                article_page_node.children.each do |node|
                  p, ff = parse_paragraph(node) do |state|
                    yield state
                  end
                  paragraphs << p
                  f << ff unless ff.nil?
                end
                paragraphs.reject! { |x| x.nil? }
                paragraphs = nil if paragraphs.empty?
              end
              if f.empty?
                f = nil
              elsif f.size == 1
                f = f.first
              end
              rr = {}
              rr[:paragraphs] = paragraphs unless paragraphs.nil?
              rr[:flattened] = f unless f.nil?
              r << rr unless rr.empty?
              ensure_empty_node article_page_node
              r
            end

            def parse_paragraph(node)
              if node.comment? and node.content.strip == 'article end'
                yield({:article_end_flag => true})
                node.unlink
                return nil
              elsif node.text?
                begin
                  text = node.content.strip.gsub(/\s+/, ' ')
                  return nil if text.empty?
                  return {:text => text}, text
                ensure
                  node.unlink
                end
              end

              # pre parse tree
              case node.name
                when 'br'
                  node.unlink
                  return nil, nil
                when 'a'
                  if not node.has_attribute?('href') and node.has_attribute?('name')
                    begin
                      return {:anchor => {:name => node.attr('name')}}, nil
                    ensure
                      node.unlink
                    end
                  end
                when 'span'
                  if node.has_attribute? 'data-widget'
                    begin
                      return {:widget => {:ticker_name => node.attr('data-ticker-name')}}, nil
                    ensure
                      node.unlink
                    end
                  end
              end

              f = []
              #parse tree
              parsed_children = []
              node.children.each do |n|
                p, ff = parse_paragraph n do |state|
                  yield state
                end
                f << ff unless ff.nil?
                last_p = parsed_children.last
                if not (last_p.nil? or p.nil?) and last_p.has_key? :text and p.has_key? :text
                  # append the text to last one
                  last_p[:text] = last_p[:text] + ' ' + p[:text]
                  next
                end
                parsed_children << p unless p.nil?
              end
              r = {:p => parsed_children}
              if f.empty?
                f = nil
              elsif f.size == 1
                f = f.first
              end

              #post parse tree
              case node.name
                when /h(\d+)/
                  r = {:heading => parsed_children, :level => $~[1].to_i}
                when 'cite'
                  r = {:cite => parsed_children}
                when 'p'
                  if node.has_attribute?('class') and node.matches? '.articleVersion'
                    r = {:article_version => parsed_children}
                    f = nil
                  else
                    raise "Unrecognized Class of node p\n#{node.inspect}" unless node.attr('class').nil?
                  end
                when 'strong'
                  r = {:strong => parsed_children}
                when 'em'
                  r = {:em => parsed_children}
                when 'blockquote'
                  r = {:block_quote => parsed_children}
                when 'no'
                  r = {:no => parsed_children}
                when 'a'
                  if node.matches? '.topicLink'
                    return {:topic_link => {:link => node.attr('href'), :_ => parsed_children}}
                  end
                  if node.has_attribute?('href')
                    begin
                      case node.attr('href')
                        when /^mailto:(.*)/
                          r = {:email => {:email_address => $~[1], :_ => parsed_children}}
                        when /^\/public\/quotes\/main\.html\?type=(?<type>\w+)&symbol=(?<symbol>[\w\.:-]+)$/
                          r = {:quote => {:type => $~[:type], :symbol => $~[:symbol], :_ => parsed_children}}
                        else
                          r = {:link => {:url => node.attr('href'), :_ => parsed_children}}
                      end
                    ensure
                      node.unlink
                    end
                  else
                    p node
                    raise "Need Developer"
                  end
                when 'div', 'span', 'li'
                  ensure_empty_node node
                when 'ul'
                  if node.matches? '.articleList'
                    r = {:article_list => parsed_children}
                  else
                    p node
                    raise 'Need Developer'
                  end
                else
                  raise 'Unrecognized node in .articlePage paragraphs: ' + "\n" + node.inspect + "\n"
              end
              ensure_empty_node node
              return r, f
            end

          end #ArticlePageParser

          class ArticleParser < HTMLParser
            @@head_meta_parser = HeadMetaParser.new
            @@article_headline_box_parser = ArticleHeadlineBoxParser.new
            @@article_page_parser= ArticlePageParser.new

            def parse(node)
              article_start_flag = false
              article_end_flag = false
              no_content = false

              article = []
              r = select_set_to_parse(node, ['head meta']) do |node_set|
                r = []
                node_set.each do |n|
                  nr = @@head_meta_parser.parse(n)
                  r << nr unless nr.nil?
                end
                {:head_meta => r}
              end
              article << r unless r.nil?
              select_only_node_to_parse(node, '.articleHeadlineBox', true) do |n|
                article += @@article_headline_box_parser.parse n do |state|
                  article_start_flag = true if state[:article_start_flag]
                end
              end
              article_story_body_node = node.css('#article_story_body').first
              if article_story_body_node.nil?
                no_content = true
                article << {:_nobody => true}
              else
                select_only_node_to_parse article_story_body_node, '.articlePage' do |article_page_node|
                  article += @@article_page_parser.parse(article_page_node) do |state|
                    article_end_flag = true if state[:article_end_flag]
                  end
                end
              end

              raise "Improper article start/end flag: start(#{article_start_flag}), end(#{article_end_flag})" unless (article_start_flag and article_end_flag) or no_content

              {:article => article}
            end
          end # class ArticleParser

        end # module Parsers

        def process_article(url, content)
          fix_article_html! content
          doc = Nokogiri::HTML(content)
          r = Parsers::ArticleParser.new.parse(doc)
          r[:url] = url

          r
        end

        def fix_article_html!(text)
          text.gsub!('<TH>', ' ')
        end

        def retrieve(date = nil, record_date = true)
          logger = Rails.logger
          if date.nil?
            # auto determine the date to be retrieved from the database
            last_processed_date_item = @state_table.items.at("wsj-last_processed_date-#{PROCESSOR_VERSION}")
            if last_processed_date_item.exists?
              date = Time.parse(last_processed_date_item.attributes['value']).in_time_zone(TIME_ZONE) + 1.day
            else
              date = TIME_ZONE.parse('2009-04-01')
            end
          end
          if date > Time.now
            date = Time.now
          end
          local_date = get_local_date date
          if (record_date)
            @state_table.items.create({'key' => "wsj-last_processed_date-#{PROCESSOR_VERSION}",
                                       'value' => local_date.utc.iso8601}) unless @test_mode
          end
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
            if false
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
        end

        def cleanup_status
          @error_table.items.each do |item|
            next unless item.hash_value == 'wsj-error'
            if item.attributes['processor_version'] < PROCESSOR_VERSION
              item.delete
              next
            end
            next unless item.attributes['processor_version'] == PROCESSOR_VERSION
            unless item.attributes['fix_patch'].nil? or item.attributes['processor_patch'] >= item.attributes['fix_patch']
              item.delete
              next
            end

            if item.attributes['processor_patch'].nil? or item.attributes['processor_patch'] < PROCESSOR_PATCH
              count = 0
              date = Time.parse(item.attributes['date'])
              retrieve date, false do |type, document|
                count += 1
                if type == :normalized_article
                  puts "R #{date}|#{count}: #{document[:url]}"
                end
              end
              item.attributes.set({'fix_patch' => PROCESSOR_PATCH})
            end
          end
        end
      end
    end
  end
end
