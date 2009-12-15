$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'
require 'forwardable'

require 'active_record/connection_adapters/abstract_adapter'

module RR

  # Enables the fetching of (potential large) result sets in chunks.
  class ResultFetcher

    # The current database ProxyConnection
    attr_accessor :connection

    # hash of select options as described under ProxyConnection#select_cursor
    attr_accessor :options

    # column_name => value hash of the last returned row
    attr_accessor :last_row

    # The current row set: an array of column_name => value hashes
    attr_accessor :rows

    # Index to the current row in rows
    attr_accessor :current_row_index

    # Creates a new fetcher.
    # * +connection+: the current ProxyConnection
    # * +options+: hash of select options as described under ProxyConnection#select_cursor
    def initialize(connection, options)
      self.connection = connection
      self.options = options.clone
    end

    # Returns +true+ if there are more rows to read.
    def next?
      unless self.rows
        # Try to load some records
        
        if options[:query] and last_row != nil
          # A query was directly specified and all it's rows were returned
          # ==> Finished.
          return false
        end

        if options[:query]
          # If a query has been directly specified, just directly execute it
          query = options[:query]
        else
          # Otherwise build the query
          if last_row
            # There was a previous batch.
            # Next batch will start after the last returned row
            options.merge! :from => last_row, :exclude_starting_row => true
          end

          query = connection.table_select_query(options[:table], options)

          if options[:row_buffer_size]
            # Set the batch size
            query = connection.add_limit_offset!(query, :limit =>options[:row_buffer_size] )
          end
        end

        self.rows = connection.select_all query
        self.current_row_index = 0
      end
      self.current_row_index < self.rows.size
    end

    # Returns the row as a column => value hash and moves the cursor to the next row.
    def next_row
      raise("no more rows available") unless next?
      self.last_row = self.rows[self.current_row_index]
      self.current_row_index += 1

      if self.current_row_index == self.rows.size
        self.rows = nil
      end

      self.last_row
    end

    # Frees up all ressources
    def clear
      self.rows = nil
    end
  end

  # This class represents a remote activerecord database connection.
  # Normally created by DatabaseProxy
  class ProxyConnection
    # Ensure that the proxy object always stays on server side and only remote
    # references are returned to the client.
    include DRbUndumped

    extend Forwardable
    
    # The database connection
    attr_accessor :connection

    # A hash as described by ActiveRecord::Base#establish_connection
    attr_accessor :config
    
    # Forward certain methods to the proxied database connection
    def_delegators :connection,
      :columns, :quote_column_name,
      :quote_table_name, :execute,
      :select_one, :select_all, :tables,
      :begin_db_transaction, :rollback_db_transaction, :commit_db_transaction,
      :referenced_tables,
      :create_or_replace_replication_trigger_function,
      :create_replication_trigger, :drop_replication_trigger, :replication_trigger_exists?,
      :sequence_values, :update_sequences, :clear_sequence_setup,
      :drop_table, :add_big_primary_key, :add_column, :remove_column

    # Caching the primary keys. This is a hash with
    #   * key: table name
    #   * value: array of primary key names
    attr_accessor :primary_key_names_cache
    
    # Hash to register cursors.
    # Purpose:
    #   Objects only referenced remotely via DRb can be garbage collected.
    #   We register them in this hash to protect them from unintended garbage collection.
    attr_accessor :cursors
    
    # 2-level Hash of table_name => column_name => Column objects.
    attr_accessor :table_columns
        
    # Hash of table_name => array of column names pairs.
    attr_accessor :table_column_names

    # A hash of manually overwritten primary keys:
    # * key: table_name
    # * value: array of primary key names
    attr_accessor :manual_primary_keys
    
    # Returns an array of primary key names for the given +table_name+.
    # Caches the result for future calls. Allows manual overwrites through
    # the Configuration options +:primary_key_names+ or :+primary_key_only_limit+.
    #
    # Parameters:
    # * +table_name+: name of the table
    # * +options+: An option hash with the following valid options:
    #   * :+raw+: if +true+, than don't use manual overwrites and don't cache
    def primary_key_names(table_name, options = {})
      return connection.primary_key_names(table_name) if options[:raw]
      
      self.primary_key_names_cache ||= {}
      result = primary_key_names_cache[table_name]
      unless result
        result = manual_primary_keys[table_name] || connection.primary_key_names(table_name)
        primary_key_names_cache[table_name] = result
      end
      result
    end
    
    # Creates a table
    # Call forwarded to ActiveRecord::ConnectionAdapters::SchemaStatements#create_table
    # Provides an empty block (to prevent DRB from calling back the client)
    def create_table(*params)
      connection.create_table(*params) {}
    end

    # Returns a Hash of currently registerred cursors
    def cursors
      @cursors ||= {}
    end
    
    # Store a cursor in the register to protect it from the garbage collector.
    def save_cursor(cursor)
      cursors[cursor] = cursor
    end

    # Returns a cusor as produced by the #select_cursor method of the connection
    # extenders.
    #
    # Two modes of operation: Either
    # * execute the specified query (takes precedense) OR
    # * first build the query based on options forwarded to #table_select_query
    # +options+ is a hash with
    # * :+query+: executes the given query
    # * :+type_cast+: if +true+, build a type casting cursor around the result
    # * :+table+: name of the table from which to read data
    # * further options as taken by #table_select_query to build the query
    # * :+row_buffer_size+:
    #   Integer controlling how many rows a read into memory at one time.
    def select_cursor(options)
      cursor = ResultFetcher.new(self, options)
      if options[:type_cast]
        cursor = TypeCastingCursor.new(self, options[:table], cursor)
      end
      cursor
    end
    
    # Create a session on the proxy side according to provided configuration hash.
    # +config+ is a hash as described by ActiveRecord::Base#establish_connection
    def initialize(config)
      self.connection = ConnectionExtenders.db_connect config
      self.config = config
      self.manual_primary_keys = {}
    end

    # Destroys the session
    def destroy
      cursors.each_key do |cursor|
        cursor.destroy
      end
      cursors.clear

      self.connection.disconnect!
    end
    
    def add_limit_offset!(query, options)
      self.connection.add_limit_offset!(query, options)
    end
    
    def savepoint(name)
      self.connection.savepoint name
    end

    def lookup_column(table, column)
      self.table_columns ||= {}
      unless table_columns.include? table
        table_columns[table] = {}
        columns(table).each {|c| 
          table_columns[table][c.name] = c
        }
      end
      
      table_columns[table][column]
    end
      
    # Quotes the given value. It is assumed that the value belongs to the specified column name and table name.
    # Caches the column objects for higher speed.
    def quote_value(table, column, value)
      connection.quote value, lookup_column(table, column)
    end
    
    # Create a cursor for the given table.
    #   * +cursor_class+: should specify the Cursor class (e. g. ProxyBlockCursor or ProxyRowCursor).
    #   * +table+: name of the table 
    #   * +options+: An option hash that is used to construct the SQL query. See ProxyCursor#construct_query for details.
    def create_cursor(cursor_class, table, options = {})
      cursor = cursor_class.new self, table
      cursor.prepare_fetch options
      save_cursor cursor
      cursor
    end
    
    # Destroys the provided cursor and removes it from the register
    def destroy_cursor(cursor)
      cursor.destroy
      cursors.delete cursor
    end
    
    # Returns an array of column names of the given table name.
    # The array is ordered in the sequence as returned by the database.
    # The result is cached for higher speed.
    def column_names(table)
      self.table_column_names ||= {}
      unless table_column_names.include? table
        table_column_names[table] = columns(table).map {|c| c.name}
      end
      table_column_names[table]
    end
  
    # Returns a list of quoted column names for the given +table+ as comma 
    # separated string.
    def quote_column_list(table)
      column_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_column_list
    
    # Returns a list of quoted primary key names for the given +table+ as comma
    # separated string.
    def quote_key_list(table)
      primary_key_names(table).map do |column_name| 
        quote_column_name(column_name)
      end.join(', ')
    end
    private :quote_key_list
    
    
    # Generates an sql condition string for the given +table+ based on
    #   * +row+: a hash of primary key => value pairs designating the target row
    #   * +condition+: the type of sql condition (something like '>=' or '=', etc.)
    def row_condition(table, row, condition)
      query_part = ""
      query_part << ' (' << quote_key_list(table) << ') ' << condition
      query_part << ' (' << primary_key_names(table).map do |key|
        quote_value(table, key, row[key])
      end.join(', ') << ')'
      query_part
    end
    private :row_condition

    # Returns an SQL query string for the given +table+ based on the provided +options+.
    # +options+ is a hash that can contain any of the following:
    #   * :+from+: nil OR the hash of primary key => value pairs designating the start of the selection
    #   * :+exclude_starting_row+: if true, do not include the row specified by :+from+
    #   * :+to+: nil OR the hash of primary key => value pairs designating the end of the selection
    #   * :+row_keys+: an array of primary key => value hashes specify the target rows.
    def table_select_query(table, options = {})
      query = "select #{quote_column_list(table)}"
      query << " from #{quote_table_name(table)}"
      query << " where" if [:from, :to, :row_keys].any? {|key| options.include? key}
      first_condition = true
      if options[:from]
        first_condition = false
        matching_condition = options[:exclude_starting_row] ? '>' : '>='
        query << row_condition(table, options[:from], matching_condition)
      end
      if options[:to]
        query << ' and' unless first_condition
        first_condition = false
        query << row_condition(table, options[:to], '<=')
      end
      if options[:row_keys]
        query << ' and' unless first_condition
        if options[:row_keys].empty?
          query << ' false'
        else
          query << ' (' << quote_key_list(table) << ') in ('
          first_key = true
          options[:row_keys].each do |row|
            query << ', ' unless first_key
            first_key = false
            query << '(' << primary_key_names(table).map do |key|
              quote_value(table, key, row[key])
            end.join(', ') << ')'
          end
          query << ')'
        end
      end
      query << " order by #{quote_key_list(table)}"

      query
    end
      
    def inner_perform_jdbc_prepared_query(query, table, values, con)
        st = con.prepareStatement(query)
        index = 0
        values.each_pair do |column_name, value|
          index = index + 1
          col = lookup_column(table, column_name.to_s)

#            puts "#{col.name} #{col.type.inspect}"
          if value.nil?
            case col.type
            when :string              
              st.setNull(index,java.sql.Types::VARCHAR)
            when :integer
              st.setNull(index,java.sql.Types::INTEGER)
            when :decimal, :float, :double
              st.setNull(index,java.sql.Types::DECIMAL)
            when :binary
              st.setNull(index,java.sql.Types::BLOB)
            when :time
              st.setNull(index,java.sql.Types::TIME)
            when :date, :datetime, :timestamp
              st.setNull(index,java.sql.Types::TIMESTAMP)
            else
              st.setNull(index, java.sql.Types::VARCHAR)
#              puts "ELSEWHERE: #{col.inspect} - #{table} - #{column_name}"
            end
          else
            case col.type
            when :text
                st.setString(index, java.lang.String.new(value.unpack('c*').to_java(:byte), 'UTF-8'))
            when :string
              if value.is_a?(String)
#                st.setString(index, value)
                st.setString(index, java.lang.String.new(value.unpack('c*').to_java(:byte), 'UTF-8'))
              else
                st.setString(index, value.to_s)
              end
            when :integer
              st.setInt(index, value)
            when :decimal, :double, :float
              st.setDouble(index, value)
            when :binary
              st.setBytes(index, value.to_java_bytes)
            when :date, :datetime, :timestamp
#            puts "#{column_name} - #{value.inspect} - #{value.class.inspect} - #{value.to_i rescue nil}"
            
              if value.is_a?(Time)
                st.setString(index, value.strftime('%Y-%m-%d-%H.%M.%S'))
#                st.setTimestamp(index, java.sql.Timestamp.new(value.to_i*1000))
              else
                if value.is_a?(String)
                  st.setString(index, value)
                else
                  if value.is_a?(Date)
                    st.setString(index, value.strftime('%Y-%m-%d'))
                    # YUCK!
#                    st.setDate(index, java.sql.Date.new(value.to_time.to_i*1000))
                  else
                    puts "Unknown object type for date field: #{value.inspect} - #{value.class.inspect}"
                    st.setObject(index, value)
                  end                  
                end
              end
            when :time
              st.setTime(index, java.sql.Time.new(value.to_i*1000))
            else
              puts "HERE: #{col.inspect} - #{table} - #{column_name} - #{value.inspect}"
              st.setObject(index, value)
            end            
          end
        end

        begin
          st.executeUpdate
        rescue
	     raise $!
        ensure
          st.close      
        end
    end
    
    def perform_jdbc_prepared_query(query, table, values)
#      puts "#{query} - #{table} - #{values.inspect}"
      x = nil
      @connection.raw_connection.with_connection_retry_guard do |con| 
        x = con
      end
      
      inner_perform_jdbc_prepared_query(query, table, values, x)

=begin
      vals = []
      types = []
      values.each_pair do |k,v|
        vals << v
        types << lookup_column(table, k.to_s).type
      end
      
      @connection.raw_connection.update_bind(query, vals, types)
=end      
    end
    
    # Returns an SQL insert query for the given +table+ and +values+.
    # +values+ is a hash of column_name => value pairs.
    def table_insert_query(table, values, for_jdbc)
      query = "insert into #{quote_table_name(table)}"
      query << '(' << values.keys.map do |column_name|
        quote_column_name(column_name)
      end.join(', ') << ') '
      query << 'values(' << values.map do |column_name, value|
        if for_jdbc
          '?'
        else
          quote_value(table, column_name, value)
        end
      end.join(', ') << ')'
      
      query
    end
    
    # Inserts the specified records into the named +table+.
    # +values+ is a hash of column_name => value pairs.
    def insert_record(table, values)
      if RUBY_PLATFORM =~ /java/
        query = table_insert_query(table, values, true)
#        puts query
        perform_jdbc_prepared_query(query, table, values)
      else
        execute table_insert_query(table, values, false)
      end
    end
    
    # Returns an SQL update query.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    def table_update_query(table, values, org_key = nil, for_jdbc = false)
      org_key ||= values
      query = "update #{quote_table_name(table)} set "
      query << values.map do |column_name, value|
        if for_jdbc
          "#{quote_column_name(column_name)} = ?"
        else
          "#{quote_column_name(column_name)} = #{quote_value(table, column_name, value)}"
        end
      end.join(', ')
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, org_key[key])
      end.join(', ') << ")"
      
      query
    end
    
    # Updates the specified records of the specified table.
    # * +table+: name of the target table
    # * +values+: a hash of column_name => value pairs.
    # * +org_key+:
    #   A hash of column_name => value pairs. If +nil+, use the key specified by
    #   +values+ instead.
    def update_record(table, values, org_key = nil)
      if RUBY_PLATFORM =~ /java/
        query = table_update_query(table, values, org_key, true)
        perform_jdbc_prepared_query(query, table, values)
      else
        execute table_update_query(table, values, org_key, false)
      end
    end

    # Returns an SQL delete query for the given +table+ and +values+
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    def table_delete_query(table, values)
      query = "delete from #{quote_table_name(table)}"
      query << " where (" << quote_key_list(table) << ") = ("
      query << primary_key_names(table).map do |key|
        quote_value(table, key, values[key])
      end.join(', ') << ")"
      
      query
    end
    
    # Deletes the specified record from the named +table+.
    # +values+ is a hash of column_name => value pairs. (Only the primary key
    # values will be used and must be included in the hash.)
    def delete_record(table, values)
      execute table_delete_query(table, values)
    end
  end
end
