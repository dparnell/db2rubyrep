RR::Initializer::run do |config|
  config.options[:verbose] = true

  config.left = {
    :adapter  => 'jdbc',
    :driver   => 'com.ibm.db2.jcc.DB2Driver',
    :url      => 'jdbc:db2://lefthost:50000/sample',
    :username => 'username',
    :password => 'password',
    :schema   => 'schema'
  }

  config.right = {
    :adapter  => 'jdbc',
    :driver   => 'com.ibm.db2.jcc.DB2Driver',
    :url      => 'jdbc:db2://righthost:50000/sample',
    :username => 'username',
    :password => 'password',
    :schema   => 'schema'
  }

  config.include_tables 'EMPLOYEE'
  # config.include_tables /^e/ # regexp matching all tables starting with e
  # config.include_tables /./ # regexp matching all tables in the database  
end
