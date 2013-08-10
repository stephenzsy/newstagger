namespace :newstagger do
  task :tag => :environment do
    require 'newstagger/vendor/reuters'

    retriever = NewsTagger::Vendor::Reuters::Retriever.new
    retriever.retrieve Time.now.utc - 1.day

  end
end