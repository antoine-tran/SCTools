StreamCorpusDexter
==================

Entity Linking Plugin for StreamCorpus using Dexter framework


This is a plugin for TREC StreamCorpus (http://trec-kba.org/kba-stream-corpus-2014.shtml) that performs entity linking for documents using Dexter (http://www.dxtr.it) framework. 


StreamCorpusIndexer
===================

Indexing TREC StreamCorpus using Terrier.

This software includes a patch for Terrier to support reading .xz files in the collection, as well as to adapt Terrier with new Hadoop framework (Hadoop YARN).

It has been tested on a Hadoop cluster running on Cloudera CDH5.1.0 (Hadoop 2.3.0), Terrier 4.0 and StreamCorpus v0.3.0.

How to install and run
===========
(More details come soon. If you can't wait to get the instruction, feel free to contact ttran AT L3S DOT de)

1. Compile: <code> ant clean fatjar</code>

2. Configure: To index the StreamCorpus, follow the same steps for configuring Terrier in Hadoop settings (http://terrier.org/docs/v4.0/hadoop_configuration.html), including the index path ("terrier.index.path") and collection path ("collection.spec"). The most important change is to specify the Terrier API for StreamCorpus collection class with the value:

     <code> trec.collection.class=de.l3s.streamcorpus.StreamCorpusCollection</code>

3. Run: <code> sh run-jars.sh bin/streamcorpus-indexer-[VERSION]-fat.jar de.l3s.streamcorpus.StreamCorpusIndexing</code>
