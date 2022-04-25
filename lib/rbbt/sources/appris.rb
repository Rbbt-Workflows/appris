require 'json'
require 'rbbt-util'
require 'rbbt/workflow'
require 'rbbt/sources/ensembl_ftp'
require 'rbbt/sources/organism'

module Appris
  extend Resource
  self.subdir = 'share/databases/Appris'

  def self.organism(organism = "Hsa")
    "#{organism}/feb2014"
  end

  def self.principal_transcripts(organism)
    org = organism.split("/").first
    build = Organism.GRC_build(organism)
    TSV.traverse Appris[org][build].principal_isoforms, :type => :array, :into => Set.new do |line|
      parts = line.split("\t")
      parts.shift
      parts.extend MultipleResult
      parts
    end
  end

  %w(Hsa Mmu Rno).each do |organism|
    codes = Organism.organism_codes(organism)
    scientific_name = Organism.scientific_name(organism).downcase.sub(" ", "_")
    builds = {}
    codes.each do |code|
      build = Organism.GRC_build(code)
      builds[build] ||= []
      builds[build] << code
    end
    builds.each do |build, orgs|
      Appris.claim Appris[organism][build].principal_isoforms, :proc do
        url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/#{scientific_name}/#{build}/appris_data.principal.txt"
        tsv = TSV.open(url, :key_field => 1, :fields => [2], :type => :flat, :merge => true, :grep => "PRINCIPAL")
        tsv.key_field = "Ensembl Gene ID"
        tsv.fields = ["Ensembl Transcript ID"]
        tsv.namespace = orgs.first
        tsv.to_s
      end

      Appris.claim Appris[organism][build].principal_isoform_proteins, :proc do
        tsv = Appris[organism][build].principal_isoforms.tsv
        tsv.swap_id("Ensembl Transcript ID", "Ensembl Protein ID", :identifiers => Organism.transcripts(orgs.last)).to_s
      end

      %w(firestar spade thump crash).each do |method|
        Appris.claim Appris[organism][build].annotations[method], :proc do
          tsv = TSV.setup({}, "Ensembl Transcript ID~Location,Feature,Feature_value#:type=:double")
          url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/#{scientific_name}/#{build}/appris_method.#{method}.gtf.gz"
          TSV.traverse Open.open(url), :type => :array do |line|
            parts = line.split("\t")
            chr, db, f = parts
            info = parts.last
            hash = {}
            info.split(";").each do |entry|
              key, value = entry.split(" ")

              hash[key] = value.gsub('"','')
            end
            hash["note"].split(",").each do |entry|
              key, value = entry.split(":")
              hash[key] = value
            end
            transcript, gene, pep_position, pep_start, pep_end = hash.values_at "transcript_id", "gene_id", "pep_position", "pep_start", "pep_end"
            feature_keys = (hash.keys - %w(transcript_id gene_id pep_position pep_start pep_end note))
            feature_key = (hash.keys & %w(ligands hmm_name)).first
            next if pep_position.nil? && pep_start.nil? && pep_end.nil?
            pep_position = [pep_start, pep_end] * "-" if pep_position.nil?
            feature = hash[feature_key] 
            tsv.zip_new transcript, [pep_position, f, feature]
          end
          tsv
        end
      end

      Appris.claim Appris[organism][build].protein_annotations, :proc do
        firestar = Appris[organism][build].annotations.firestar.tsv
        spade = Appris[organism][build].annotations.spade.tsv
        thump = Appris[organism][build].annotations.thump.tsv
        crash = Appris[organism][build].annotations.crash.tsv
        tsv = firestar
        tsv = tsv.merge_zip spade
        tsv = tsv.merge_zip thump
        tsv = tsv.merge_zip crash
        tsv
      end
    end

  end

  def self.principal_isoforms_for_organism(code)
    build = Organism.GRC_build(code)
    organism = code.partition("/").first
    Appris[organism][build].principal_isoforms.tsv
  end

  def self.principal_isoform_proteins_for_organism(code)
    build = Organism.GRC_build(code)
    organism = code.partition("/").first
    Appris[organism][build].principal_isoform_proteins.tsv
  end

  Appris.claim Appris.principal_isoforms_mmu, :proc do
    #url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/homo_sapiens/g19v24/appris_data.principal.txt"
    url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/homo_sapiens/GRCh38/appris_data.principal.txt"
    tsv = TSV.open(url, :key_field => 1, :fields => [2], :type => :flat, :merge => true, :grep => "PRINCIPAL")
    tsv.key_field = "Ensembl Gene ID"
    tsv.fields = ["Ensembl Transcript ID"]
    tsv.namespace = Appris.organism
    tsv.to_s
  end

  Appris.claim Appris.principal_isoforms, :proc do
    url = "http://apprisws.bioinfo.cnio.es/pub/current_release/datafiles/homo_sapiens/GRCh38/appris_data.principal.txt"
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

  def self.ensg2principal_enst
    @ensg2principal_enst ||= begin
                               Organism.transcripts(self.organism).index :tsv_grep => PRINCIPAL_TRANSCRIPTS.to_a, :target => "Ensembl Transcript ID", :persist => true, :fields => ["Ensembl Gene ID"]
                             end
  end

  def self.ensg2principal_ensp
    @ensg2principal_ensp ||= begin
                               Organism.transcripts(self.organism).index :tsv_grep => PRINCIPAL_TRANSCRIPTS.to_a, :target => "Ensembl Protein ID", :persist => true, :fields => ["Ensembl Gene ID"]
                             end
  end
end


if __FILE__ == $0
  Log.severity = 0

  Log.tsv Appris.Rno["Rnor_6.0"].protein_annotations.produce(true).tsv
end
