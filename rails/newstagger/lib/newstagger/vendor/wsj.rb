require 'newstagger/retriever/retriever'
require 'cgi'
require 'newstagger/retriever/parser'

require 'aws-sdk'

module NewsTagger
  module Vendor
    module WSJ
      class Retriever < NewsTagger::Retriever::S3CachedRetriever

        WEBSITE_VERSION = '20130825'
        PROCESSOR_VERSION = '20130909'
        PROCESSOR_PATCH = 1
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

        def recursive_remove_if_empty!(node)
          if node.text?
            node.remove if node.text.strip.empty?
            return
          end
          node.children.each do |child|
            recursive_remove_if_empty! child
          end
          node.remove if node.children.empty?
        end

        def validate_all_processed(e, name)
          e.children.each do |element|
            case element.type
              when Nokogiri::XML::Node::TEXT_NODE
                next if element.text.strip.empty?
            end
            raise "Unrecognized #{name} element:\n#{element.inspect()[0..1024]}"
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
              when 'phrase'
                texts << element.text.strip
                result[:entities] ||= []
                entity = {}
                element.attributes.each do |name, value|
                  entity[name.to_sym] = value
                end
                result[:entities] << entity
                element.remove
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
          elsif element.name == 'li' or element.name == 'em'
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
                  (not fw_sibling.nil? and (fw_sibling.name == 'strong' or fw_sibling.name == 'em'))
                case fw_sibling.text.strip
                  when '', '|'
                    fw_sibling.remove
                    next
                  when /in (.*) and/, /in (.*),?/, /at (.*),?/
                    result[:location] = $~[1].strip
                  when /from (.*)/i
                    result[:organization] = $~[1].strip
                  else
                    result[:entity] = fw_sibling.text.strip
                end
                fw_sibling.remove
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

          # clean up
          social_by_line.css('li.connect').remove
          social_by_line.css('strong').each do |node|
            node.before(node.children)
            node.remove
          end
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
                  result<< {:entity => line}
              end
              element.remove
            end
          end
          return nil if result.empty?
          result
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

          class ArticleHeadlineBoxParser < HTMLParser

            def parse(node)
              r = []
              c_metadata = []
              select_only_node_to_parse(node, ['ul.cMetadata']) do |c_metadata_node|
                c_metadata << select_only_node_to_parse(c_metadata_node, ['li.articleSection']) do |article_section_node|
                  r_article_section = {:name => article_section_node.text.strip}
                  select_only_node_to_parse article_section_node, ['a'], true do |a|
                    r_article_section[:link] = a.attr('href')
                  end
                  {:article_section => r_article_section}
                end
                c_metadata << select_only_node_to_parse(c_metadata_node, ['.dateStamp'], false) do |date_stamp|
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
                end
              end
              r << {:c_metadata => c_metadata} unless c_metadata.empty?
              select_only_node_to_parse(node, 'h1') do |h1|
                r << {:headline => h1.text.strip}
              end
              select_only_node_to_parse(node, 'h2.subhead') do |h2|
                r << {:subhead => h2.text.strip}
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

          class SocialBylineParser < HTMLParser

            class By_li_star_Rule < NewsTagger::Parsers::ParserRules::RuleBase
              def validate?(node_seq, parent_node)
                return false unless parent_node.element? and parent_node.name == 'ul'
                state = :S
                node_seq.each do |node|
                  if node.text? and node.content.strip.empty?
                    node.unlink
                    next
                  end
                  case state
                    when :S
                      if node.text? and node.content.strip.downcase == 'by'
                        state = :by
                        next
                      end
                      return false
                    when :by, :li_separator
                      if node.element? and node.name == 'li'
                        state = :li
                        next
                      end
                      return false
                    when :li
                      if node.text? and ['and'].include? node.content.strip.downcase
                        state = :li_separator
                        next
                      end
                      return false
                  end
                end
                case state
                  when :li
                    return true
                end
                false
              end

              def parse(node_seq)
                r = []
                state = :S
                node_seq.each do |node|
                  case state
                    when :S
                      if node.text? and node.content.strip.downcase == 'by'
                        state = :by
                        next
                      end
                      return false
                    when :by, :li_separator
                      if node.element? and node.name == 'li'
                        r << {:author => parse_li(node)}
                        state = :li
                        next
                      end
                    when :li
                      if node.text? and ['and'].include? node.content.strip.downcase
                        state = :li_separator
                        next
                      end
                  end
                end
                r.reject! { |e| e.nil? }
                node_seq.unlink
                return nil if r.empty?
                r
              end

              def parse_li(node)
                r = []
                node.attributes.each do |name, attr|
                  if name.match /data-(\S+)/
                    r << {:data => {:name => $~[1], :value => attr.content}}
                    attr.unlink
                  end
                end
                if node.children.size == 1
                  child_node = node.children.first
                  if child_node.text?
                    r << {:name => child_node.content.strip}
                    child_node.unlink
                  elsif child_node.name == 'a'
                    attribute_value = child_node.attr('href')
                    r << {:link => attribute_value} unless attribute_value.nil?
                    cc_node = child_node.children.first
                    if cc_node.text?
                      r << {:name => cc_node.content.strip}
                      cc_node.unlink
                    end
                    ensure_empty_node child_node
                  end
                end
                ensure_empty_node node
                r.reject! { |x| x.nil? }
                return r
              end
            end

            def parse(node)
              node.css('#connectButton').unlink
              node_seq = node.children
              matched = false
              begin
                [By_li_star_Rule.new].each do |rule|
                  if rule.validate? node_seq, node
                    matched = true
                    r = rule.parse node_seq
                    return nil if r.nil?
                    ensure_empty_node node
                    return {:social_byline => r}
                  end
                end
                raise "No rule matches the .socialByline node sequence.\n#{node_seq.inspect}"
              ensure
                node_seq.unlink if matched
              end
            end
          end

          class ArticlePageParser < HTMLParser
            @@social_byline_parser = SocialBylineParser.new

            def parse(article_page_node)
              r = []
              social_byline = select_only_node_to_parse article_page_node, ['.socialByline'] do |node|
                @@social_byline_parser.parse node
              end
              r << social_byline unless social_byline.nil?
              paragraphs = []
              begin
                article_page_node.children.each do |node|
                  if node.comment? and node.content.strip == 'article end'
                    yield({:article_end_flag => true})
                    node.unlink
                    next
                  end
                  paragraphs << parse_paragraph(node)
                end
                paragraphs.reject! { |x| x.nil? }
                paragraphs = nil if paragraphs.empty?
              end
              r << {:paragraphs => paragraphs} unless paragraphs.nil?
              ensure_empty_node article_page_node
              r
            end

            def parse_paragraph(node)
              if node.text?
                begin
                  text = node.content.strip.gsub(/\s+/, ' ')
                  return nil if text.empty?
                  return {:text => text}
                ensure
                  node.unlink
                end
              end
              # pre parse tree
              case node.name
                when 'p' # handled by post
                when 'a'
                  if node.has_attribute?('href')
                    case node.attr('href')
                      when /^mailto:(.*)/
                        raise "Need Developer"
                      when /^\/public\/quotes\/main\.html\?type=(?<type>\w+)&symbol=(?<symbol>[\w\.:-]+)$/
                        raise "Need Developer"
                      else
                        raise "Need Developer"
                    end
                  elsif node.has_attribute?('name')
                    begin
                      return {:anchor => {:name => node.attr('name')}}
                    ensure
                      ensure_empty_node node
                    end
                  else
                    raise "Need Developer"
                  end
                else
                  raise 'Need Developer: ' + "\n" + node.inspect
              end

              #parse tree
              parsed_children = []
              node.children.each do |n|
                p = parse_paragraph n
                last_p = parsed_children.last
                if  not last_p.nil? and last_p.has_key? :text and p.has_key? :text
                  # append the text to last one
                  last_p[:text] = last_p[:text] + ' ' + p[:text]
                  next
                end
                parsed_children << p unless n.nil?
              end
              r = {:p => parsed_children}

              #post parse tree
              case node.name
                when 'p'
                  if node.has_attribute?('class')
                    if node.matches? '.articleVersion'
                      r = {:article_version => parsed_children}
                    else
                      raise "Unrecognized Class of node p\n#{node.inspect}" unless node.attr('class').nil?
                    end
                  end
                when 'a' # handled by pre
                else
                  raise 'Need Developer: ' + "\n" + node.inspect
              end
              ensure_empty_node node
              r
            end

          end #ArticlePageParser

          class ArticleParser < HTMLParser
            @@head_meta_parser = HeadMetaParser.new
            @@article_headline_box_parser = ArticleHeadlineBoxParser.new
            @@article_page_parser= ArticlePageParser.new

            def parse(node)
              article_start_flag = false
              article_end_flag = false

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
              article += select_only_node_to_parse(node, ['.articleHeadlineBox'], true) do |n|
                @@article_headline_box_parser.parse n do |state|
                  article_start_flag = true if state[:article_start_flag]
                end
              end
              article_story_body_node = node.css('#article_story_body').first
              if article_story_body_node.nil?
                article << {:_nobody => true}
              else
                select_only_node_to_parse article_story_body_node, ['.articlePage'], true do |article_page_node|
                  article += @@article_page_parser.parse(article_page_node) do |state|
                    article_end_flag = true if state[:article_end_flag]
                  end
                end
              end

              raise "Improper article start/end flag: start(#{article_start_flag}), end(#{article_end_flag})" unless (article_start_flag and article_end_flag)

              {:article => article}
            end
          end # class ArticleParser


        end

        def process_article(url, content)
          fix_article_html! content
          doc = Nokogiri::HTML(content)
          article = Parsers::ArticleParser.new.parse(doc)

          return article

          puts JSON.pretty_generate(results)
          raise 'Need Developer'

          parsed_metadata = {}

          doc.css(".articlePage").each do |article_page|
            article_page.css('ul.socialByline').each do |social_by_line|
              by = nil
              begin
                by = process_social_by_line social_by_line
                social_by_line.remove if validate_all_processed social_by_line, '.columnist .social_by_line'
              rescue
                by = social_by_line.text.gsub("\n", ' ').squeeze(' ').strip
                social_by_line.remove
              end
              article[:by] = by unless by.nil?
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
              case element.name
                when 'div'
                  element.css('.offDutyMoreSection').remove
                  element.remove if validate_all_processed element, '.articlePage div'
                  next
                when 'table'
                  recursive_remove_if_empty! element
                  # TODO parse table
                  element.remove
                  next
              end
            end

            article_page.remove if validate_all_processed article_page, '.articlePage'
          end

          raise "Improper article start/end flag: start(#{article_start_flag}), end(#{article_end_flag})" unless (article_start_flag and article_end_flag) or
              (article[:_no_head] and article[:_no_body])

          article
        end

        def fix_article_html!(text)
          text.gsub!('<TH>', ' ')
        end

        def fix_article_page!(article_page)
          article_page.css('.legacyInset').each do |legacy_insert|
            legacy_insert.css('.insetContent').remove
            legacy_insert.before(legacy_insert.children)
            legacy_insert.remove if validate_all_processed legacy_insert, '.legacyInset'
          end
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
