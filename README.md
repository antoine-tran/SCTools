StreamCorpus Tools
==================

SCTools is a small set of tools to preprocess the [StreamCorpus dataset](http://s3.amazonaws.com/aws-publicdatasets/trec/ts/index.html) in Hadoop MapReduce environment. Current task includes (still updating):


==========
###### Terrier Indexing: ######


Indexing TREC StreamCorpus in Hadoop mode using Terrier

This software includes a patch for Terrier to support reading .xz files in the collection, as well as to adapt Terrier with new Hadoop framework (Hadoop YARN).

It has been tested on a Hadoop cluster running on Cloudera CDH5.1.0 (Hadoop 2.3.0), Terrier 4.0 and StreamCorpus v0.3.0.



1. Configuration: After compiling using Maven compile task, you need to configure Terrier in Hadoop settings (see [here](http://terrier.org/docs/v4.0/hadoop_configuration.html)). The most important values are the index path ("terrier.index.path") and collection path ("collection.spec"). To index StreamCorpus collection, please specify the Terrier API for StreamCorpus collection class with the value as follows:

     <code> trec.collection.class=de.l3s.streamcorpus.StreamCorpusCollection</code>

2. Run: Run with the run-jars.sh script: 

<code> sh run-jars.sh bin/sctools-[VERSION].jar de.l3s.streamcorpus.mapreduce.TerrierIndexing</code>

==========
###### Massive Entity Annotation: ######

Annotating massively the StreamCorpus documents with state-of-the-art named entity disambiguation algorithms such as TagMe or WikiMiner. It relies on great annotation toolkit [Dexter](http://dexter.isti.cnr.it)

1. Configuration: To be able to use this feature, first you have to store the configuration file dexter-conf.xml and the model files of Dexter in your shared locations (e.g. HDFS or HBase file). In dexter-conf.xml, please specify the full path of the model with the protocol (file:/// or http:// or hdfs://). For example, 

```
<model>
 <name>en</name>
 <path>hdfs://[YOUR_HADOOP_CLUSTER_HOST]/[PATH-TO-English-model-directory]</path>
</model>
```

where the INPATH is the path in shared locations of your data (e.g. HDFS), OUTPATH is where you want to store the annotation results (CSV format: docid TAB [list of <entity,score> pairs]), and "dexter-conf" is the location of the configuration file dexter-conf.xml. If there is no protocol specified, it will assume the path to be in local file system.



2. Run: After compiling with Maven, just run the run-jars.sh script:

<code> sh run-jars.sh bin/sctools-[VERSION].jar de.l3s.streamcorpus.mapreduce.Annotation -in [INPATH] -out [OUTPATH] -dexter [dexter-conf]</code>
