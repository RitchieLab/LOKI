bin_PROGRAMS = pop_loader

pop_loader_SOURCES = ldsplineimporter.cpp \
	ldsplineimporter.h \
	main.cpp
	
pop_loader_LDADD = ./ldspline/src/ldspline/libldspline.la

AM_CPPFLAGS = -I$(srcdir)/ldspline/src

bin_SCRIPTS = buildPopulations.py

EXTRA_DIST = buildPopulations.py
 
	