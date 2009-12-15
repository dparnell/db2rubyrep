module RR
  module ConnectionExtenders

    module DB2Extender
      RR::ConnectionExtenders.register :db2 => self
      
      # *** Monkey patch***
      def tables(name = nil)
        options = RR::Configuration.instance.options
        rr_tables = %w{ running_flags pending_changes logged_events }.collect do |t|
          "'#{options[:rep_prefix]}_#{t}'".upcase
        end.join(',')
        
        select_all("SELECT trim(tabschema)||'.'||trim(tabname) as tablename FROM syscat.tables union all SELECT trim(tabname) as tablename FROM syscat.tables where trim(tabname) in (#{rr_tables})", name).map { |row| row['tablename'].downcase }
      end
            
      def columns(table_name,  name = nil)
        if table_name.split('.').length==1
          where = "trim(tabschema)||'.'||trim(tabname)=current_schema||'.#{table_name.upcase}'"
        else
          where = "trim(tabschema)||'.'||trim(tabname)='#{table_name.upcase}'"
        end
  
        cols = select_all("select colname, default, typename||'('||trim(char(length))||')' type, nulls from syscat.columns where #{where} order by colno")
        
        cols.collect do |row|
          ActiveRecord::ConnectionAdapters::Column.new(row['colname'].downcase, row['default'], row['type'], row['nulls']=='Y')
        end
      end
      
      def add_limit_offset!(query, options)
        "#{query} fetch first #{options[:limit]} rows only"
      end
      
      def savepoint(name)
        execute("savepoint #{name} on rollback retain cursors")
      end

      def primary_key_names(table)
        if tables.grep(/^#{table}$/i).empty?
          # Note: Cannot use tables.include? as returned tables are made lowercase under JRuby MySQL
          raise "table '#{table}' does not exist"
        end
        columns = []
        parts = table.split('.')
        if parts.length==2
          schema = parts[0]
          table = parts[1]
        else
          schema = nil
        end
        
        result_set = @connection.connection.getMetaData.getPrimaryKeys(nil, schema, table.upcase);
        while result_set.next
          column_name = result_set.getString("COLUMN_NAME").downcase
          key_seq = result_set.getShort("KEY_SEQ")
          columns << {:column_name => column_name, :key_seq => key_seq}
        end
        columns.sort! {|a, b| a[:key_seq] <=> b[:key_seq]}
        key_names = columns.map {|column| column[:column_name]}
        key_names
      end
      
      
      def quote(value, column = nil) # :nodoc:        
        if column && column.type == :primary_key
          return value.to_s
        end
        if column && (column.type == :decimal || column.type == :integer) && value
          return value.to_s
        end
        case value
        when String
          if column && column.type == :binary
            if value.length==0
              'cast(null as blob)'
            else
              "0x#{value.unpack('H*')[0]}"
            end
          else
            "'#{quote_string(value)}'"
          end
        else super
        end
      end
      
      def quote_string(string)
        string.gsub(/'/, "''")
      end
      
    end
  end
end