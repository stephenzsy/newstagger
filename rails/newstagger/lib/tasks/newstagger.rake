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
      until (not t.nil? and t >= Time.now) or Time.now > start_time + 10.minutes do
        count = 0
        retriever.retrieve t do |local_date, document|
          count += 1
          Rails.logger.debug "#{local_date.strftime("%Y-%m-%d")}:#{count}P: #{document[:url]}"
          t = local_date
        end
        t += 1.day
      end
    end
  end
end
