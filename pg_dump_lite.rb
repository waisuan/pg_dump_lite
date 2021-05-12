require 'pg'
require_relative 'config'

class PgDumpLite
  def reset
    dest_db.exec("DROP SCHEMA public CASCADE;")
    dest_db.exec("CREATE SCHEMA public;")
    dest_db.exec("GRANT ALL ON SCHEMA public TO postgres;")
    dest_db.exec("GRANT ALL ON SCHEMA public TO #{Config.dest_db_username};")
    dest_db.exec("GRANT ALL ON SCHEMA public TO public;")
  end

  def copy_schema
    command = "PGPASSWORD=#{Config.src_db_password} pg_dump -U #{Config.src_db_username} -h #{Config.src_db_host} --schema-only #{Config.src_db_name}" \
                "| PGPASSWORD=#{Config.dest_db_password} psql -h #{Config.dest_db_host} -U #{Config.dest_db_username} #{Config.dest_db_name}"
    run_cmd(command)
  end

  def copy_full_tables
    Config.full_tables.each do |table|
      command = "PGPASSWORD=#{Config.src_db_password} pg_dump -U #{Config.src_db_username} -h #{Config.src_db_host} -a -t #{table} #{Config.src_db_name}" \
                  "| PGPASSWORD=#{Config.dest_db_password} psql -h #{Config.dest_db_host} -U #{Config.dest_db_username} #{Config.dest_db_name}"
      run_cmd(command)
    end
  end

  def copy_partial_tables
    init_tmp_dir
    Config.partial_tables.each do |table|
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
    run_cmd("mkdir #{Config.tmp_dirname}")
  end

  def rm_tmp_dir
    run_cmd("rm -rf #{Config.tmp_dirname}/")
  end

  def rm_file(file)
    run_cmd("rm #{file}")
  end

  def export_partial_tables_from_src(table_name, export_statement)
    export_file = "#{Config.tmp_dirname}/#{table_name}.csv"
    command = %{
                 PGPASSWORD=#{Config.src_db_password} psql -U #{Config.src_db_username} -h #{Config.src_db_host} #{Config.src_db_name}
                 -c "\\COPY (#{export_statement}) TO '#{export_file}' WITH (DELIMITER ',', FORMAT CSV)"
               }.then { |x| compact_multiline_string(x) }
    run_cmd(command)
    export_file
  end

  def import_partial_tables_into_dest(table_name, export_file)
    command = %{
                 PGPASSWORD=#{Config.dest_db_password} psql -U #{Config.dest_db_username} -h #{Config.dest_db_host} #{Config.dest_db_name}
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
    @dest_db ||= PG.connect(:host => Config.dest_db_host, :dbname => Config.dest_db_name, :user => Config.dest_db_username, :password => Config.dest_db_password)
  end
end

# Run!
pg_dump_lite = PgDumpLite.new
pg_dump_lite.reset
pg_dump_lite.copy_schema
pg_dump_lite.copy_full_tables
pg_dump_lite.copy_partial_tables
