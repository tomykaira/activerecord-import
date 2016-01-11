require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require File.expand_path(File.dirname(__FILE__) + '/../support/postgresql/import_examples')

pg_version = ENV["PG_VERSION"] || "0"

should_support_postgresql_import_functionality
should_support_postgresql_upsert_functionality if pg_version >= '9.5'
