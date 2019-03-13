This is repository includes an implementation of Hsiao, Chung, Shen, and Chao, "Load Rebalancing for Distributed File Systems in Clouds" (IEEE Transcations on Parallel and Distributed Systems, May 2013)
on top of Hadoop HDFS.

The load balancing implementation is written by Zeitan Liu, with some revisions by Charles Reiss, under the supervision of Haiying Shen.

It incorporates a Chord implementation written Chuan Xiu which is available under an MIT license (see below).

For an example of how to use this, see a seperate repository for experiment running scripts [to be linked].

We would like to thank the collabarators of this project:

*  Bruce Maggs, Pelham Wilder Professor of Computer Science, Duke University and Vice President, Research, Akamai Technologies
*  Weijia Xu, Research Engineer, Texas Advanced Computing Center 
*  Open Science Grid (OSG)
*  SURA (Southeastern Universities Research Association) 
*  Naval Research Lab

# Files modified

All modifications are in hadoop-hdfs, including

*  a new package org.apache.hadoop.hdfs.server.datanode.chord.*
*  modifications to the DataNode, BPOfferService, FsDatasetAsyncDiskService, and Namenode classes

You can obtain a diff from the "stock" version of Hadoop by using the git history of this
repository.

# Chord implementation license

Chord implementation copyright (c) 2015-2019 Chuan Xia

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
