namespace :newstagger do
  namespace :tag do
    task :reuters => :environment do
      require 'newstagger/vendor/reuters'

      retriever = NewsTagger::Vendor::Reuters::Retriever.new
      retriever.retrieve Time.now.utc - 2.day

    end

    task :bloomberg => :environment do
      require 'newstagger/vendor/bloomberg'

      retriever = NewsTagger::Vendor::Bloomberg::Retriever.new
      t = Time.new(2010, 03, 04).utc
      until t >= Time.now do
        retriever.retrieve t do |document|
          p document[:url]
        end
        t += 1.day
      end


    end

    task :wsj => :environment do
      require 'newstagger/vendor/wsj'

      retriever = NewsTagger::Vendor::WSJ::Retriever.new

      start_time = Time.now
      t = nil
      until (not t.nil? and t >= Time.now) or Time.now > start_time + 15.minutes do
        count = 0
        retriever.retrieve t do |type, value|
          case type
            when :date
              t = value
            when :normalized_article
              count += 1
              document = value
              Rails.logger.debug "#{t.strftime("%Y-%m-%d")}:#{count}P: #{document[:url]}"
          end
        end
        t += 1.day
      end
    end

    task :test_wsj => :environment do
      require 'newstagger/vendor/wsj'

      retriever = NewsTagger::Vendor::WSJ::Retriever.new :test_mode => true

      t = ActiveSupport::TimeZone['America/New_York'].parse('2009-06-20')
      count = 0
      retriever.retrieve t do |type, value|
        case type
          when :date
            t = value
          when :normalized_article
            count += 1
            document = value
            puts "#{t.strftime("%Y-%m-%d")}:#{count}P: #{document[:url]}"
        end
      end
    end

    task :wsj_cleanup => :environment do

      require 'newstagger/vendor/wsj'

      retriever = NewsTagger::Vendor::WSJ::Retriever.new
      timeout(3600) do
        retriever.cleanup_status
      end
    end

  end
end
