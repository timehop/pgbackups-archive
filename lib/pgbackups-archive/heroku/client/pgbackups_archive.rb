require "heroku/client"
require "tmpdir"

class Heroku::Client::PgbackupsArchive

  attr_reader :client, :pgbackup

  def self.perform
    backup = new
    backup.capture
    backup.download
    backup.archive
    backup.delete
  end

  def initialize(attrs={})
    Heroku::Command.load
    @client   = Heroku::Client::Pgbackups.new pgbackups_url
    @pgbackup = nil
  end

  def archive
    PgbackupsArchive::Storage.new(key, file).store
  end

  def capture
    tries ||= 50
    @pgbackup = @client.create_transfer(database_url, database_url, nil,
      "BACKUP", :expire => true)

    until @pgbackup["finished_at"]
      print "."
      sleep 1

      begin
        @pgbackup = @client.get_transfer @pgbackup["id"]
      rescue RestClient::ServiceUnavailable, RestClient::InternalServerError => error
        print "\nTemporary error getting status of backup. Retrying in 10 seconds... #{error}"
        sleep 10
        if (tries -= 1).zero?
          raise error
        else
          retry
        end
      rescue RestClient::ResourceNotFound => error
        raise StandardError.new("Cannot find backup! There is probably a backup already in progress. Run `heroku pgbackups:destroy ID` to destroy the existing backup. #{@pgbackup.inspect}")
      end
    end
  end

  def delete
    File.delete temp_file
  end

  def download
    File.open(temp_file, "wb") do |output|
      streamer = lambda do |chunk, remaining_bytes, total_bytes|
        output.write chunk
      end
      Excon.get(@pgbackup["public_url"], :response_block => streamer)
    end
  end

  private

  def database_url
    ENV["PGBACKUPS_DATABASE_URL"] || ENV["DATABASE_URL"]
  end

  def environment
    defined?(Rails) ? Rails.env : nil
  end

  def file
    File.open temp_file, "r"
  end

  def key
    ["pgbackups", environment, @pgbackup["finished_at"]
      .gsub(/\/|\:|\.|\s/, "-").concat(".dump")].compact.join("/")
  end

  def pgbackups_url
    ENV["PGBACKUPS_URL"]
  end

  def temp_file
    "#{Dir.tmpdir}/#{URI(@pgbackup['public_url']).path.split('/').last}"
  end

end
