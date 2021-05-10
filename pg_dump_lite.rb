require 'pg'
require 'yaml'

class PgDumpLite

  def reset
    dest_db.exec("DROP SCHEMA public CASCADE;")
    dest_db.exec("CREATE SCHEMA public;")
    dest_db.exec("GRANT ALL ON SCHEMA public TO postgres;")
    dest_db.exec("GRANT ALL ON SCHEMA public TO public;")
  end

  def copy_schema
    command = "PGPASSWORD=#{src_db_password} pg_dump -U #{src_db_username} -h #{src_db_host} --schema-only #{src_db_name}" \
                "| PGPASSWORD=#{dest_db_password} psql -h #{dest_db_host} -U #{dest_db_username} #{dest_db_name}"
    run_cmd(command)
  end

  def copy_full_tables
    full_tables.each do |table|
      command = "PGPASSWORD=#{src_db_password} pg_dump -U #{src_db_username} -h #{src_db_host} -a -t #{table} #{src_db_name}" \
                  "| PGPASSWORD=#{dest_db_password} psql -h #{dest_db_host} -U #{dest_db_username} #{dest_db_name}"
      run_cmd(command)
    end
  end

  def copy_partial_tables
    tmp_dir = "tmp"
    run_cmd("mkdir #{tmp_dir}")
    partial_tables.each do |table|
      table_name = table.keys.first
      partial_select = table[table_name]
      command = %{
                    PGPASSWORD=#{src_db_password} psql -U #{src_db_username} -h #{src_db_host} #{src_db_name}
                    -c "\\COPY (#{partial_select}) TO '#{tmp_dir}/#{table_name}.csv' WITH (DELIMITER ',', FORMAT CSV)"
                 }.then { |x| compact_multiline_string(x) }
      run_cmd(command)

      command = %{
                    PGPASSWORD=#{dest_db_password} psql -U #{dest_db_username} -h #{dest_db_host} #{dest_db_name}
                    -c "\\COPY #{table_name} FROM '#{tmp_dir}/#{table_name}.csv' CSV"
                 }.then { |x| compact_multiline_string(x) }
      run_cmd(command)
    end
    run_cmd("rm -rf #{tmp_dir}/")
  end

  private

  def compact_multiline_string(string)
    string.gsub(/\s+/, " ").strip
  end

  def run_cmd(cmd)
    system(cmd)
  end

  def dest_db
    @dest_db ||= PG.connect(:host => dest_db_host, :dbname => dest_db_name, :user => dest_db_username, :password => dest_db_password)
  end

  def src_db_username
    config["SRC_DB_USERNAME"]
  end

  def src_db_host
    config["SRC_DB_HOST"]
  end

  def src_db_name
    config["SRC_DB_NAME"]
  end

  def src_db_password
    config["SRC_DB_PASSWORD"]
  end

  def dest_db_username
    config["DEST_DB_USERNAME"]
  end

  def dest_db_host
    config["DEST_DB_HOST"]
  end

  def dest_db_name
    config["DEST_DB_NAME"]
  end

  def dest_db_password
    config["DEST_DB_PASSWORD"]
  end

  def full_tables
    config["FULL_TABLES"]
  end

  def partial_tables
    config["PARTIAL_TABLES"]
  end

  def config
    @config_file ||= YAML.load_file('config.yaml')
  end
end

pg_dump_lite = PgDumpLite.new
pg_dump_lite.reset
pg_dump_lite.copy_schema
pg_dump_lite.copy_full_tables
pg_dump_lite.copy_partial_tables
