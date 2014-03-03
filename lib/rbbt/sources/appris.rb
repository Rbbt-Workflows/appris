require 'json'
require 'rbbt/resource'
require 'rbbt/workflow'
require 'rbbt/sources/ensembl_ftp'

Workflow.require_workflow "Genomics"
require 'rbbt/entity/gene'
require 'rbbt/entity/transcript'
require 'rbbt/entity/protein'

module Appris
  extend Resource
  self.subdir = 'share/databases/Appris'

  Appris.claim Appris.principal_isoforms, :proc do
    url = "http://appris.bioinfo.cnio.es/download/data/appris_data.principal.homo_sapiens.tsv.gz"
    tsv = TSV.open(url, :key_field => 1, :fields => [2], :type => :flat, :merge => true)
    tsv.key_field = "Ensembl Gene ID"
    tsv.fields = ["Ensembl Transcript ID"]
    tsv.namespace = "Hsa/jan2013"
    tsv.to_s
  end

  PRINCIPAL_ISOFORMS = Persist.persist("Appris principal isoforms", :marshal){ Set.new(Transcript.setup(Appris.principal_isoforms.tsv.values.compact.flatten, "Ensembl Transcript ID", "Hsa/jan2013").protein.compact.sort.uniq) }

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

module Gene

  property :appris_release => :single do
    Appris.rbbt2appris_release organism
  end


  property :appris_gene_info => :single do
    begin
      info = JSON.parse(Open.read("http://appris.bioinfo.cnio.es/ws/#{appris_release}/rest/export/id/#{self.ensembl}?source=appris&format=json"))
    rescue
      raise "No Appris info on gene: #{self.name || self }"
    end

    tsv = TSV.setup({}, :key_field => "Ensembl Transcript ID", :type => :list, :fields => ["Name", "Status", "Biotype", "Principal Isoform?"])

    info.each do |hash|
      tsv[hash["transcript_id"]] = hash.values_at *["transcript_name", "status", "biotype", "annotation"]
    end

    tsv.entity_options = {:organism => self.organism}

    index = Organism.transcripts(organism).tsv(:persist => true, :fields => ["Ensembl Protein ID"], :type => :single)
    tsv.add_field "Ensembl Protein ID" do |key, values|
      index[key]
    end

    tsv.namespace = organism

    tsv
  end

  property :principal_isoforms => :single do

    url = "http://appris.bioinfo.cnio.es/ws/latest/rest/export/id/#{self.ensembl}?source=appris&format=json"
    begin
      info = JSON.parse(Open.read(url))

      transcript_annotations = {}

      info.each do |hash|
        transcript_annotations[hash["transcript_id"]] = hash["annotation"]
      end

      Transcript.setup(transcript_annotations.select{|trans, annot| annot == "Principal Isoform" }.collect{|trans, annot| trans}, "Ensembl Transcript ID", organism)
    rescue
      Log.warn "Principal isoforms not found in Appris: #{self}"
      nil
    end
  end
end

module Protein
  property :appris_release => :single do
    Appris.rbbt2appris_release organism
  end

  property :appris_residues => :single do
    begin
      info = JSON.parse(Open.read("http://appris.bioinfo.cnio.es/ws/#{appris_release}/rest/residues/id/#{self.transcript}"))
      info[self.transcript]
    rescue
      nil
    end
  end
end
