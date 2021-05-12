# Context
The aim of this tool is to provide an easy + quick way of extracting a specified subset of data from one database and importing the extracted "slice" into another. This can be especially useful for local development where a developer would need to import a "slice" of production data into their local DB env.

# Configure
All that you'd need to touch / care about is in ```config.yaml```.

# Execute
```
bundle install
bundle exec ruby pg_dump_lite.rb
```
