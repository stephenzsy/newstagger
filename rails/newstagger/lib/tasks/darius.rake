namespace :darius do
  def on_ec2_instance?
    output = `ec2-metadata 2>&1`
    return true if $?.exitstatus == 0
    false
  end

  task :on_ec2 => :environment do
    p on_ec2_instance?
  end
end
