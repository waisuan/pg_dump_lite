require 'yaml'

class Config
  class << self
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
end
