require 'json'
require 'rbbt/resource'
require 'rbbt/workflow'
require 'rbbt/sources/ensembl_ftp'

module Appris
  extend Resource
  self.subdir = 'share/databases/Appris'

  def self.organism
    "Hsa/jan2013"
  end

  Appris.claim Appris.principal_isoforms, :proc do
    url = "http://appris.bioinfo.cnio.es/download/data/appris_data.principal.homo_sapiens.tsv.gz"
    tsv = TSV.open(url, :key_field => 1, :fields => [2], :type => :flat, :merge => true)
    tsv.key_field = "Ensembl Gene ID"
    tsv.fields = ["Ensembl Transcript ID"]
    tsv.namespace = "Hsa/jan2013"
    tsv.to_s
  end

  PRINCIPAL_TRANSCRIPTS = Persist.persist("Appris principal transcripts", :marshal){ Set.new Appris.principal_isoforms.tsv.values.compact.flatten }
  PRINCIPAL_ISOFORMS = Persist.persist("Appris principal isoforms", :marshal){ 
    index = Organism.transcripts(organism).index :target => "Ensembl Protein ID", :fields => ["Ensembl Transcript ID"], :unnamed => true
    Set.new index.chunked_values_at(PRINCIPAL_TRANSCRIPTS.to_a)
  }

  def self.ensembl2appris_release(release)
    return 'latest' if release == 'current'
    num = release.split("-").last.to_i
    case
    when num > 62
      "latest"
    when num > 52
      "rel7"
    else
      "rel3c"
    end
  end

  def self.rbbt2appris_release(organism)
    release = organism.index("/") ? Ensembl.releases[organism.split("/").last] : 'current'
    ensembl2appris_release(release)
  end
end

