$LOAD_PATH.unshift(File.dirname(__FILE__) + '/lib') if __FILE__ == $0
require 'rbbt/workflow'
require 'rbbt/sources/appris'

module Appris
  extend Workflow
end
