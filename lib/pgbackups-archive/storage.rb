require "fog"
require "open-uri"

class PgbackupsArchive::Storage

  # Default multipart_chunk_size => 5mb
  def initialize(key, file, multipart_chunk_size=5242880)
    @key = key
    @file = file
    @multipart_chunk_size = multipart_chunk_size
  end

  def connection
    Fog::Storage.new({
      :provider              => "AWS",
      :aws_access_key_id     => ENV["PGBACKUPS_AWS_ACCESS_KEY_ID"],
      :aws_secret_access_key => ENV["PGBACKUPS_AWS_SECRET_ACCESS_KEY"],
      :region                => ENV["PGBACKUPS_REGION"],
      :persistent            => false
    })
  end

  def bucket
    connection.directories.get ENV["PGBACKUPS_BUCKET"]
  end

  def store
    bucket.files.create :key => @key, :body => @file, :public => false, :multipart_chunk_size => multipart_chunk_size
  end
end
