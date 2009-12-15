RR::Initializer::run do |config|
  config.options[:verbose] = true
  config.options[:maintain_activity_status] = false
  
  config.left = {
    :adapter  => 'jdbc',
    :driver   => 'com.ibm.db2.jcc.DB2Driver',
    :url      => 'jdbc:db2://127.0.0.1:50001/seo',
    :username => 'clintel',
    :password => 'vyp552',
    :schema   => 'TSINSTALL'
  }

  config.right = {
    :adapter  => 'jdbc',
    :driver   => 'com.ibm.db2.jcc.DB2Driver',
    :url      => 'jdbc:db2://127.0.0.1:50001/seo2',
    :username => 'clintel',
    :password => 'vyp552',
    :schema   => 'TSINSTALL'
  }

  config.include_tables 'TSINSTALL.F_NOTES', :key => 'note_rsn'

#  config.include_tables 'TSINSTALL.F_PATIENT', :key => 'pt_rsn'
end
