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
        retriever.retrieve t  do |document|
          p document[:url]
        end
        t += 1.day
      end

    end
  end

  task :tag_all => ["newstagger:tag:reuters", "newstagger:tag:bloomberg"] do
  end

end
