$LOAD_PATH.unshift(File.dirname(__FILE__) + '/lib') if __FILE__ == $0
require 'rbbt/workflow'
require 'rbbt/sources/appris'

module Appris
  extend Workflow

  input :genes, :array, "Genes"
  input :organism, :string, "Organism code", "Hsa"
  task :principal_transcripts => :tsv do |genes,organism|
    index = Organism.identifiers(organism).index :target => "Ensembl Gene ID", :persist => true
    ensg2enst = Organism.gene_transcripts(organism).tsv :fields => ["Ensembl Transcript ID"], :type => :flat, :persist => true
    dumper = TSV::Dumper.new :key_field => "Gene", :fields => ["Ensembl Transcript ID"], :type => :flat, :namespace => organism
    dumper.init
    TSV.traverse genes, :into => dumper, :type => :array do |gene|
      ensg = index[gene]
      if ensg
        enst = ensg2enst[ensg]
        if enst
          enst = PRINCIPAL_TRANSCRIPTS & enst
          [gene, enst.to_a]
        else
          [gene, []]
        end
      else
        [gene, []]
      end
    end
  end

  dep :principal_transcripts
  task :principal_isoforms => :tsv do 
    organism = step(:principal_transcripts).inputs[:organism]
    enst2ensp = Organism.transcripts(organism).index :target => "Ensembl Protein ID", :fields => ["Ensembl Transcript ID"], :persist => true
    dumper = TSV::Dumper.new :key_field => "Gene", :fields => ["Ensembl Protein ID"], :type => :flat, :namespace => organism
    dumper.init
    TSV.traverse step(:principal_transcripts), :into => dumper do |gene, transcripts|
      proteins = enst2ensp.values_at(*transcripts).compact.uniq
      [gene, proteins]
    end
  end

  export_asynchronous :principal_transcripts, :principal_isoforms
end
