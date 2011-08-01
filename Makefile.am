ACLOCAL_AMFLAGS = -I config
AUTOMAKE_OPTIONS = 1.9 foreign

bin_PROGRAMS = dnet_convert_meta dnet_convert_history dnet_convert_files blob_unsort

dnet_convert_meta_SOURCES = convert_meta.c common.c

dnet_convert_history_SOURCES = convert_history.c common.c

if HAVE_BOOST_FILESYSTEM
if HAVE_BOOST_PROGRAM_OPTIONS
if HAVE_BOOST_IOSTREAMS
if HAVE_BOOST_THREAD

AM_CXXFLAGS = @BOOST_CPPFLAGS@

dnet_convert_files_SOURCES = convert_files.cpp common.c
dnet_convert_files_LDADD = @BOOST_LDFLAGS@ @BOOST_SYSTEM_LIB@ @BOOST_IOSTREAMS_LIB@ \
				@BOOST_THREAD_LIB@ @BOOST_FILESYSTEM_LIB@ @BOOST_PROGRAM_OPTIONS_LIB@

blob_unsort_SOURCES = blob_unsort.cpp
blob_unsort_LDADD = @BOOST_LDFLAGS@ @BOOST_SYSTEM_LIB@ @BOOST_IOSTREAMS_LIB@

endif
endif
endif
endif

