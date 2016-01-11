module ActiveRecord::Import::PostgreSQLAdapter
  include ActiveRecord::Import::ImportSupport
  include ActiveRecord::Import::OnDuplicateKeyUpdateSupport

  def insert_many( sql, values, *args ) # :nodoc:
    number_of_inserts = 1

    base_sql,post_sql = if sql.is_a?( String )
      [ sql, '' ]
    elsif sql.is_a?( Array )
      [ sql.shift, sql.join( ' ' ) ]
    end

    sql2insert = base_sql + values.join( ',' ) + post_sql
    ids = select_values( sql2insert, *args )

    ActiveRecord::Base.connection.query_cache.clear

    [number_of_inserts,ids]
  end

  def next_value_for_sequence(sequence_name)
    %{nextval('#{sequence_name}')}
  end

  def post_sql_statements( table_name, options ) # :nodoc:
    unless options[:primary_key].blank?
      super(table_name, options) << (" RETURNING #{options[:primary_key]}")
    else
      super(table_name, options)
    end
  end

  # Add a column to be updated on duplicate key update
  def add_column_for_on_duplicate_key_update( column, options={} ) # :nodoc:
    if options[:on_duplicate_key_update].is_a?( Hash )
      columns = options[:on_duplicate_key_update].fetch(:columns, [])
      case columns
      when Array then columns << column.to_sym unless columns.include?( column.to_sym )
      when Hash then columns[column.to_sym] = column.to_sym
      end
    end
  end

  # Returns a generated ON CONFLICT DO UPDATE statement given the passed
  # in +options+.
  def sql_for_on_duplicate_key_update( table_name, options={} ) # :nodoc:
    return unless options.is_a?( Hash )

    key = Array( options[:key] ).join( ', ' )
    raise ArgumentError, 'Attribute :key not found' if key.empty?
    columns = options.fetch(:columns) { raise ArgumentError, 'Attribute :columns not found' }

    sql = " ON CONFLICT (#{key}) DO "
    if columns.respond_to?(:empty?) and columns.empty?
      return sql << 'NOTHING'
    else
      sql << 'UPDATE SET '
    end

    if columns.is_a?( Array )
      sql << sql_for_on_duplicate_key_update_as_array( table_name, columns )
    elsif columns.is_a?( Hash )
      sql << sql_for_on_duplicate_key_update_as_hash( table_name, columns )
    elsif columns.is_a?( String )
      sql << columns
    else
      raise ArgumentError, "Expected :columns to be an Array or Hash"
    end
    sql
  end

  def sql_for_on_duplicate_key_update_as_array( table_name, arr )  # :nodoc:
    results = arr.map do |column|
      qc = quote_column_name( column )
      "#{qc}=EXCLUDED.#{qc}"
    end
    results.join( ',' )
  end

  def sql_for_on_duplicate_key_update_as_hash( table_name, hsh ) # :nodoc:
    results = hsh.map do |column1, column2|
      qc1 = quote_column_name( column1 )
      qc2 = quote_column_name( column2 )
      "#{qc1}=EXCLUDED.#{qc2}"
    end
    results.join( ',')
  end

  # Return true if the statement is a duplicate key record error
  def duplicate_key_update_error?(exception)# :nodoc:
    exception.is_a?(ActiveRecord::StatementInvalid) && exception.to_s.include?('duplicate key')
  end

  def support_setting_primary_key_of_imported_objects?
    true
  end
end
