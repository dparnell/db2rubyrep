module RR
  module ReplicationExtenders

    # Provides PostgreSQL specific functionality for database replication
    module Db2Replication
      RR::ReplicationExtenders.register :db2 => self
      
      # Returns the key clause that is used in the trigger function.
      # * +trigger_var+: should be either 'NEW' or 'OLD'
      # * +params+: the parameter hash as described in #create_rep_trigger
      def key_clause(trigger_var, params)
        params[:keys].
          map { |key| "'#{key}#{params[:key_sep]}' || trim(char(#{trigger_var}.#{key}))"}.
          join(" || '#{params[:key_sep]}' || ")
      end
      private :key_clause

      def trigger_base_name(table_name)
        options = RR::Configuration.instance.options
        
        parts = table_name.split('.')
        if parts.length==2
          "#{options[:rep_prefix]}_#{parts[1]}"
        else
          "#{options[:rep_prefix]}_#{table_name}"
        end
      end
      
      def create_or_replace_replication_trigger_function(params)
        # do nothing
      end
      
      # Creates a trigger to log all changes for the given table.
      # +params+ is a hash with all necessary information:
      # * :+trigger_name+: name of the trigger
      # * :+table+: name of the table that should be monitored
      # * :+keys+: array of names of the key columns of the monitored table
      # * :+log_table+: name of the table receiving all change notifications
      # * :+activity_table+: name of the table receiving the rubyrep activity information
      # * :+key_sep+: column seperator to be used in the key column of the log table
      # * :+exclude_rr_activity+:
      #   if true, the trigger will check and filter out changes initiated by RubyRep
      def create_replication_trigger(params)

        trigger_base = trigger_base_name(params[:table])
        
        %w(insert update delete).each do |action|
          case action
          when 'insert':
            reference = 'NEW AS NEW'
            change_key = key_clause('NEW', params)
            change_new_key = 'null'
            change_type = 'I'
          when 'update':
            reference = 'NEW AS NEW OLD AS OLD'
            change_key = key_clause('OLD', params)
            change_new_key = key_clause('NEW', params)
            change_type = 'U'
          when 'delete':
            reference = 'OLD AS OLD'
            change_key = key_clause('OLD', params)
            change_new_key = 'null'
            change_type = 'D'
          end
          
          if config[:schema]
            schema = "#{config[:schema]}."
          else
            schema = ''
          end
          
          sql = <<-end_sql
            CREATE TRIGGER #{schema}#{trigger_base}_#{action}
              AFTER #{action} ON #{params[:table]} REFERENCING #{reference} FOR EACH ROW 
              WHEN (application_id()<>#{schema}#{params[:rep_prefix]}_app_var)
              BEGIN ATOMIC
                  INSERT INTO #{schema}#{params[:log_table].upcase}(change_table, change_key, change_new_key, change_type, change_time)
                    VALUES('#{params[:table]}', #{change_key}, #{change_new_key}, '#{change_type}', current_timestamp);
              END
          end_sql
          
#          puts sql
          
          execute sql
        end
      end

      # Removes a trigger and related trigger procedure.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table for which the trigger exists
      def drop_replication_trigger(trigger_name, table_name)
        trigger_base = trigger_base_name(table_name)

        %w(INSERT UPDATE DELETE).each do |action|
          execute "DROP TRIGGER #{trigger_base}_#{action}"
        end
      end

      # Returns +true+ if the named trigger exists for the named table.
      # * +trigger_name+: name of the trigger
      # * +table_name+: name of the table
      def replication_trigger_exists?(trigger_name, table_name)
        trigger_base = trigger_base_name(table_name).upcase
        sql = "select 1 from syscat.triggers where trigname = '#{trigger_base}_INSERT'"
        !select_all(sql).empty?
      end

      # Returns all unadjusted sequences of the given table.
      # Parameters:
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table
      # Return value: a hash with
      # * key: sequence name
      # * value: a hash with
      #   * :+increment+: current sequence increment
      #   * :+value+: current value
      def sequence_values(rep_prefix, table_name)
        parts = table_name.split('.')
        
        if parts.length==1
          sql = "select * from syscat.colidentattributes where tabname='#{table_name}'"
        else
          sql = "select * from syscat.colidentattributes where tabschema='#{parts[0]}' and tabname='#{parts[1]}'"
        end
        
        result = {}
        select_all(sql).each do |row|
          result[row['colname']] = {
            :increment => row['increment'].to_i,
            :value => select_value("select max(#{row['colname']}) from #{table_name}").to_i
          }
        end

        result
      end

      # Ensures that the sequences of the named table (normally the primary key
      # column) are generated with the correct increment and offset.
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table (not used for Postgres)
      # * +increment+: increment of the sequence
      # * +offset+: offset
      # * +left_sequence_values+:
      #    hash as returned by #sequence_values for the left database
      # * +right_sequence_values+:
      #    hash as returned by #sequence_values for the right database
      # * +adjustment_buffer+:
      #    the "gap" that is created during sequence update to avoid concurrency problems
      # E. g. an increment of 2 and offset of 1 will lead to generation of odd
      # numbers.
      def update_sequences(
          rep_prefix, table_name, increment, offset,
          left_sequence_values, right_sequence_values, adjustment_buffer)
          
        left_sequence_values.each do |sequence_name, left_current_value|            
          max_current_value = [left_current_value[:value], right_sequence_values[sequence_name][:value]].max + adjustment_buffer
          new_start = max_current_value - (max_current_value % increment) + increment + offset

          puts "Altering column #{table_name} #{sequence_name} start: #{new_start} increment #{increment}"

          execute("ALTER TABLE #{table_name} ALTER COLUMN #{sequence_name} SET GENERATED BY DEFAULT")
          execute("ALTER TABLE #{table_name} ALTER COLUMN #{sequence_name} SET INCREMENT BY #{increment} RESTART WITH #{new_start}")

          commit_db_transaction
        end
      end

      # Restores the original sequence settings.
      # (Actually it sets the sequence increment to 1. If before, it had a
      # different value, then the restoration will not be correct.)
      # * +rep_prefix+: not used (necessary) for the Postgres
      # * +table_name+: name of the table
      def clear_sequence_setup(rep_prefix, table_name)
        parts = table_name.split('.')
        
        if parts.length==1
          select = "select * from syscat.colidentattributes where tabname='#{table_name}'"
        else
          select = "select * from syscat.colidentattributes where tabschema='#{parts[0]}' and tabname='#{parts[1]}'"
        end
        
        select_all(select).each do |row|
          execute("ALTER TABLE #{table_name} ALTER COLUMN #{row['colname']} SET INCREMENT BY 1")
        end
      end

      # Adds a big (8 byte value), auto-incrementing primary key column to the
      # specified table.
      # * table_name: name of the target table
      # * key_name: name of the primary key column
      def add_big_primary_key(table_name, key_name)
        execute(<<-end_sql)
          alter table #{table_name} add column #{key_name} BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 20)
        end_sql
      end

    end
  end
end

