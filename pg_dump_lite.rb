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
    init_tmp_dir
    partial_tables.each do |table|
      table_name = table.keys.first
      export_statement = table.values.first
      export_file = export_partial_tables_from_src(table_name, export_statement)
      import_partial_tables_into_dest(table_name, export_file)
        .then { |_| rm_file(export_file) }
   end
    rm_tmp_dir
  end

  private

  def init_tmp_dir
    run_cmd("mkdir #{tmp_dirname}")
  end

  def rm_tmp_dir
    run_cmd("rm -rf #{tmp_dirname}/")
  end

  def rm_file(file)
    run_cmd("rm #{file}")
  end

  def export_partial_tables_from_src(table_name, export_statement)
    export_file = "#{tmp_dirname}/#{table_name}.csv"
    command = %{
                 PGPASSWORD=#{src_db_password} psql -U #{src_db_username} -h #{src_db_host} #{src_db_name}
                 -c "\\COPY (#{export_statement}) TO '#{export_file}' WITH (DELIMITER ',', FORMAT CSV)"
               }.then { |x| compact_multiline_string(x) }
    run_cmd(command)
    export_file
  end

  def import_partial_tables_into_dest(table_name, export_file)
    command = %{
                 PGPASSWORD=#{dest_db_password} psql -U #{dest_db_username} -h #{dest_db_host} #{dest_db_name}
                 -c "\\COPY #{table_name} FROM '#{export_file}' CSV"
               }.then { |x| compact_multiline_string(x) }
    run_cmd(command)
  end

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

  def tmp_dirname
    config["TEMP_OUTPUT_DIRNAME"]
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
