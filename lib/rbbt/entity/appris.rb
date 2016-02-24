module Gene

  property :appris_release => :single do
    Appris.rbbt2appris_release organism
  end


  property :appris_gene_info => :single do
    begin
      organism_name = "homo_sapiens"
      info = JSON.parse(Open.read("http://apprisws.bioinfo.cnio.es/rest/exporter/id/#{organism_name}/#{self.ensembl}?format=json"))

    rescue
      raise "No Appris info on gene: #{self.name || self }"
    end

    tsv = TSV.setup({}, :key_field => "Ensembl Transcript ID", :type => :list, :fields => ["Name", "Status", "Biotype", "Principal Isoform?"])

    info.each do |hash|
      next unless hash["type"] == "principal_isoform"
      values = hash.values_at *["transcript_name", "status", "biotype", "annotation"]
      tsv[hash["transcript_id"]] ||= values.collect{|v| v == "No Principal Isoform" ? "Not Principal Isoform" : v }
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

    organism_name = "homo_sapiens"
    url = "http://apprisws.bioinfo.cnio.es/rest/exporter/id/#{organism_name}/#{self.ensembl}?source=appris&format=json"
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
      organism_name = "homo_sapiens"
      info = JSON.parse(Open.read("http://apprisws.bioinfo.cnio.es/rest/sequencer/id/#{organism_name}/#{self.transcript}")).first
      info[self.transcript]
    rescue Exception
      Log.exception $!
      nil
    end
  end
end
