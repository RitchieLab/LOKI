noinst_LTLIBRARIES=liblokiupdate.la

liblokiupdate_la_SOURCES=dbsettings.py\
bio_loader.py \
loaders/ncbi_loader.py \
loaders/load_reactome.py \
loaders/load_pharmgkb.py \
loaders/load_pfam.py \
loaders/load_netpath.py \
loaders/load_mint.py \
loaders/load_kegg.py \
loaders/load_go.py \
loaders/load_ensembl.py \
loaders/load_dip.py \
loaders/load_chainfiles.py \
loaders/load_biogrid.py \
loaders/util/settings.py \
loaders/util/region_manager.py \
loaders/util/biosettings.py \
loaders/util/bioloader.py 

if LOKI_INSTALL
LOKI_CMDS=$(srcdir)/src/bio_loader.py LOAD knowledge.bio ALL
else
LOKI_CMDS=touch knowledge.bio variations variations.txt
endif

nodist_data_DATA=knowledge.bio variations variations.txt

CLEANFILES=knowledge.bio variations variations.txt

variations: knowledge.bio

variations.txt : knowledge.bio

knowledge.bio: liblokiupdate.la
	rm -rf knowledge.bio
	$(LOKI_CMDS)
	chmod a-wx knowledge.bio variations variations.txt


