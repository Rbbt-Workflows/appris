require 'json'
require 'rbbt/entity/gene'
require 'rbbt/entity/transcript'
require 'rbbt/sources/ensembl_ftp'

module Appris
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
    info = JSON.parse(Open.read("http://appris.bioinfo.cnio.es/ws/#{appris_release}/rest/export/id/#{self.ensembl}?source=appris&format=json"))

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

    info = JSON.parse(Open.read("http://appris.bioinfo.cnio.es/ws/latest/rest/export/id/#{self.ensembl}?source=appris&format=json"))

    transcript_annotations = {}

    info.each do |hash|
      transcript_annotations[hash["transcript_id"]] = hash["annotation"]
    end

    Transcript.setup(transcript_annotations.select{|trans, annot| annot == "Principal Isoform" }.collect{|trans, annot| trans}, "Ensembl Transcript ID", organism)
  end
end

module Protein
  property :appris_release => :single do
    Appris.rbbt2appris_release organism
  end

  property :appris_residues => :single do
    info = JSON.parse(Open.read("http://appris.bioinfo.cnio.es/ws/#{appris_release}/rest/residues/id/#{self.transcript}"))
    info[self.transcript]
  end
end

