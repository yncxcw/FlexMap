# Yarn-SBlock
The prototype of our research project FlexMap. FlexMap is a performance heterogeneous mitigation Hadoop MapReduce 
designed for hardware heterogeneous cluster and virtualized cluster.

## Motivation and Design
Previous research has found performance heterogeneous is a killer for mapreduce. Our experiments found homogeneous 
assumption for map tasks would lead significant performance degradation.  

We design and implement FlexMap which dynamic sizing map size at runtime to adapt to node capacity. The key idea of 
FlexMap is to assign data to each map task based on node speed. You can refer our IPDPS 2017 paper for more details.

## Usage
1. Download Hadoop source code from http://hadoop.apache.org/. Our implementation is based on Haddop-2.6.0
2. Copy folder hadoop-mapreduce-project and hadoop-hdfs to hadoop source folder.
3. Compile and install(refer BUILD.txt)

## Contact
If you have any questions, please contact ynjassion@gmail.com
