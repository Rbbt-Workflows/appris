require 'json'
require 'rbbt-util'
require 'rbbt/workflow'
require 'rbbt/sources/ensembl_ftp'

module Appris
  extend Resource
  self.subdir = 'share/databases/Appris'

  def self.organism
    "Hsa/dec2013"
  end

  Appris.claim Appris.principal_isoforms, :proc do
    #url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/homo_sapiens/g19v24/appris_data.principal.txt"
    url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/homo_sapiens/GRCh37/appris_data.principal.txt"
    tsv = TSV.open(url, :key_field => 1, :fields => [2], :type => :flat, :merge => true, :grep => "PRINCIPAL")
    tsv.key_field = "Ensembl Gene ID"
    tsv.fields = ["Ensembl Transcript ID"]
    tsv.namespace = Appris.organism
    tsv.to_s
  end

  Appris.claim Appris.principal_isoform_proteins, :proc do
    tsv = Appris.principal_isoforms.tsv
    tsv.swap_id("Ensembl Transcript ID", "Ensembl Protein ID", :identifiers => Organism.transcripts(Appris.organism)).to_s
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
