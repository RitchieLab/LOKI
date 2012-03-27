noinst_LTLIBRARIES=liblokiupdate.la

liblokiupdate_la_SOURCES=dbsettings.py\
bio_loader.py \
__init__.py \
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
loaders/__init__.py \
loaders/util/settings.py \
loaders/util/region_manager.py \
loaders/util/biosettings.py \
loaders/util/bioloader.py \
loaders/util/__init__.py 

if LOKI_INSTALL
LOKI_CMDS=$(srcdir)/bio_loader.py LOAD knowledge.bio ALL
VAR_FILES=knowledge.bio variations variations.txt

variations: knowledge.bio

variations.txt : knowledge.bio

knowledge.bio: liblokiupdate.la
	rm -rf knowledge.bio
	$(LOKI_CMDS)
	chmod a-wx knowledge.bio variations variations.txt
	
else
LOKI_CMDS=$(srcdir)/bio_loader.py LOAD test.bio TEST
VAR_FILES=test.bio variations-test variations-test.txt

variations-test: test.bio

variations-test.txt: test.bio

test.bio: liblokiupdate.la
	rm -rf test.bio
	$(LOKI_CMDS)
		
endif

nodist_data_DATA=$(VAR_FILES)

CLEANFILES=test.bio knowledge.bio variations* observed_relationships.csv






