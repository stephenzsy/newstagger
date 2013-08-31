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
      #t = Time.new(2009, 4, 1).utc
      t = Time.new(2009, 4, 3).utc
      until t >= Time.now do
        puts "Day begin #{t.iso8601}"
        count = 0
        retriever.retrieve t do |document|
          count += 1
          puts "#{t.strftime("%Y-%m-%d")}:#{count}P: #{document[:url]}"
        end
        t += 1.day
        puts "Day done #{t.iso8601}"
      end

    end
  end
end
