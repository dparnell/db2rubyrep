require 'timeout'

module RR
  
  # Executes a single replication run
  class ReplicationRun

    # The current Session object
    attr_accessor :session

    # The current TaskSweeper
    attr_accessor :sweeper

    # Returns the current ReplicationHelper; creates it if necessary
    def helper
      @helper ||= ReplicationHelper.new(self)
    end

    # Returns the current replicator; creates it if necessary.
    def replicator
      @replicator ||=
        Replicators.replicators[session.configuration.options[:replicator]].new(helper)
    end

    # Executes the replication run.
    def run
      return unless [:left, :right].any? do |database|
        changes_pending = false
        t = Thread.new do
          config = session.configuration.send(database)
          
          if config[:schema]
            schema = "#{config[:schema]}."
          else
            schema = ''
          end
          
          changes_pending = session.send(database).select_one(            
            "select id from #{schema}#{session.configuration.options[:rep_prefix]}_pending_changes"
          ) != nil
        end
        t.join session.configuration.options[:database_connection_timeout]
        changes_pending
      end

      # Apparently sometimes above check for changes takes already so long, that
      # the replication run times out.
      # Check for this and if timed out, return (silently).
      return if sweeper.terminated?

      loaders = LoggedChangeLoaders.new(session)

      success = false
      begin
        replicator # ensure that replicator is created and has chance to validate settings

        loop do
          loaders.update # ensure the cache of change log records is up-to-date

          begin
            diff = ReplicationDifference.new loaders
            diff.load
            break unless diff.loaded?
            break if sweeper.terminated?
            replicator.replicate_difference diff if diff.type != :no_diff
          rescue Exception => e
            begin
              helper.log_replication_outcome diff, e.message,
                e.class.to_s + "\n" + e.backtrace.join("\n")
            rescue Exception => _
              # if logging to database itself fails, re-raise the original exception
              raise e
            end
          end
        end
        
        # TODO: add code here to commit the session if a change has been made so allowing autocommit to be turned off
        success = true
      ensure
        if sweeper.terminated?
          helper.finalize false
          session.disconnect_databases
        else
          helper.finalize success
        end
      end
    end

    # Installs the current sweeper into the database connections
    def install_sweeper
      [:left, :right].each do |database|
        unless session.send(database).respond_to?(:sweeper)
          session.send(database).send(:extend, NoisyConnection)
        end
        session.send(database).sweeper = sweeper
      end
    end

    # Creates a new ReplicationRun instance.
    # * +session+: the current Session
    # * +sweeper+: the current TaskSweeper
    def initialize(session, sweeper)
      self.session = session
      self.sweeper = sweeper
      install_sweeper
    end
  end
end
