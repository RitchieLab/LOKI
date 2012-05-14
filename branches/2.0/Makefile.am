SUBDIRS = loadPops

noinst_LTLIBRARIES=liblokiupdate.la

liblokiupdate_la_SOURCES=loki_db.py \
loki.py \
loki_source_biogrid.py \
loki_source_dbsnp.py \
loki_source_entrez.py \
loki_source_go.py \
loki_source_kegg.py \
loki_source_mint.py \
loki_source_netpath.py \
loki_source_pfam.py \
loki_source_pharmgkb.py \
loki_source.py \
loki_source_reactome.py


if LOKI_INSTALL
LOKI_CMDS=$(srcdir)/loki.py -k knowledge.bio
VAR_FILES=knowledge.bio
	
else
LOKI_CMDS=$(srcdir)/loki.py -k test.bio -u none
VAR_FILES=test.bio
	
endif

knowledge.bio: liblokiupdate.la .creation
	rm -rf knowledge.bio
	$(LOKI_CMDS)

test.bio: liblokiupdate.la .creation
	rm -rf test.bio
	$(LOKI_CMDS)
	
.creation:
	date +"%Y.%m.%d" > .creation

DATE_PREFIX:=$(shell cat .creation).

install-data-hook:
	for f in $(VAR_FILES); do \
		mv -f $(DESTDIR)$(datadir)/$$f $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f; \
		chmod a-wx $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f; \
		ln -s $(DESTDIR)$(datadir)/$(DATE_PREFIX)$$f $(DESTDIR)$(datadir)/$$f; \
	done

uninstall-hook:
	rm -f $(DESTDIR)$(datadir)/*test.bio $(DESTDIR)$(datadir)/*variations* $(DESTDIR)$(datadir)/*knowledge.bio

nodist_data_DATA=$(VAR_FILES)

CLEANFILES=test.bio knowledge.bio .creation








