noinst_LTLIBRARIES=liblokiupdate.la

liblokiupdate_la_PYTHON=dbsettings.py\
src/settings.py \
src/region_manager.py \
src/pydot.py \
src/plot_group_relationships.py \
src/ncbi_loader.py \
src/load_reactome.py \
src/load_pharmgkb.py \
src/load_pharmgkb-orig.py \
src/load_pfam.py \
src/load_netpath.py \
src/load_mint.py \
src/load_kegg.py \
src/load_hprd.py \
src/load_go.py \
src/load-entrezgene-history.py \
src/load_ensembl.py \
src/load_dip.py \
src/load_chainfiles.py \
src/load_biogrid.py \
src/biosettings.py \
src/bioloader.py \
src/bio_loader.py

if LOKI_INSTALL
LOKI_CMDS=./src/bio_loader.py LOAD knowledge.bio ALL
else
LOKI_CMDS=touch knowledge.bio variations variations.txt
endif

nodist_data_DATA=knowledge.bio variations variations.txt

knowledge.bio:

variations:

variations.txt:

install-data: pre-install-data

pre-install-data:
	rm -rf knowledge.bio
	$(LOKI_CMDS)
	chmod a-wx knowledge.bio variations variations.txt


