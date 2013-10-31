$LOAD_PATH.unshift(File.dirname(__FILE__) + '/lib') if __FILE__ == $0
require 'rbbt/workflow'
require 'appris'

module Appris
  extend Workflow

end

g = Gene.setup("ZDHHC8", "Associated Gene Name", "Hsa")

if __FILE__ == $0
  puts g.appris_gene_info["ENST00000320602"]["Ensembl Protein ID"].info
end



