echo "Pulling the images of the dockers from the Docker registry"
docker pull izone/hadoop
docker pull mongo
echo "Done"
echo "Starting the 2 Docker containers"
docker run -dit --name=mongoDB -p 80:80 -p 27017:27017 mongo
docker run -dit --name=hadoop -p 8088:8088 -p 8042:8042 -p 50070:50070 -p 8888:8888 -p 4040:4040 izone/hadoop 
echo "Done"
echo "Downloading Spark 2.1.1"
wget https://d3kbcqa49mib13.cloudfront.net/spark-2.1.1-bin-hadoop2.7.tgz --no-check-certificate
echo "Done"
echo "Extracting Spark archive"
tar -xvzf spark-2.1.1-bin-hadoop2.7.tgz
echo "Done"
echo "Copying Spark inside the hadoop Container" 
docker cp spark-2.1.1-bin-hadoop2.7 hadoop:/usr/local/spark-2.1.1-bin-hadoop2.7
echo "Done"
echo "Copying the set hdfs script inside hadoop Container"
docker cp set_hdfs.sh hadoop:/set_hdfs.sh
echo "Done"
echo "Copying the create_database script inside the mongo Container" 
docker cp create_database.sh mongoDB:/create_database.sh
echo "Done"
echo "Copying input files inside the Containers"
tar -xvzf dbInput/globalterrorismdb_0616dist.tar.gz -C dbInput
docker cp dbInput/globalterrorismdb_0616dist.csv mongoDB:/globalterrorismdb_0616dist.csv
echo "Done"
echo "Copying Jars inside hadoop Container"
docker cp Jars/mongo-spark-connector_2.10-2.0.0.jar hadoop:/mongo-spark-connector_2.10-2.0.0.jar
docker cp Jars/mongo-java-driver-3.4.2.jar hadoop:/mongo-java-driver-3.4.2.jar
docker cp Jars/ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hadoop:/ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar
echo "Done"
echo "Removing the downloaded files"
rm -rf spark-2.1.1-bin-hadoop2.7.tgz 
rm -rf spark-2.1.1-bin-hadoop2.7
echo "Done"