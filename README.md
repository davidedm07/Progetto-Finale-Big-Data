# Progetto-Finale-Big-Data

Corso di Big Data 2016/2017. 

Lo scopo del progetto è quello di creare un sistema poliglotta in grado di eseguire analisi su database di diversa natura.
Per testare il sistema, basta clonare il progetto ed avviare lo script create_dockers.sh:

  bash create_dockers.sh
  
Lo script effettua il provisioning delle macchine Docker, carica sul container mongoDB il database GTS, 
carica i jar di connessione e del progetto Java sul container hadoop.

A questo punto, bisogna accedere al terminale della macchina mongoDB lanciando:
  
  docker exec -it mongoDB bash
  
Da qui dobbiamo eseguire il comando:

  bash create_database.sh 
 
che avrà l'effetto di caricare il GTD su mongoDB (nel database dbTerr e collection attacks).

Si possono a questo punto avviare le analisi basilari dal container hadoop dopo esservi acceduti con:
  
  docker exec -it hadoop bash
  
e aver creato alcune cartelle per organizzare l'HDFS:

  hdfs dfs -mkdir /input
  
  hdfs dfs -mkdir /output
  
Le analisi possono essere avviate con i comandi (trovandosi nella cartella /usr/local/spark-2.1.1-bin-hadoop2.7/bin):

Most Attacked Countries

./spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.MostAttackedCountries" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/WDIData.csv hdfs://localhost:9000/output

Attacks Per Year

./spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.AttacksPerYear" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/WDIData.csv hdfs://localhost:9000/output/attacksDefenseExpenditure.csv

Claimed Attacks

./spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.ClaimedAttacks" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/WDIData.csv hdfs://localhost:9000/output

I riultati verrano salvati su mongoDB.
Entrando nel bash di hadoop ci troveremo nella directory /root. In essa lo script iniziale ha caricato il file WDIData.csv. Una volta caricato questo dataset su HDFS lanciando il comando:

  hdfs dfs -put WDIData.csv /input
  
potremo avviare anche le analisi poliglotte tra i due sistemi di storage con i seguenti comandi:

Attacks Energy

./spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.AttacksEnergy" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/WDIData.csv hdfs://localhost:9000/output/attacksEnergy

Attacks Defense Expenditure

./spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.AttacksDefenseExpenditure" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/WDIData.csv hdfs://localhost:9000/output/attacksDefenseExpenditure
  
I risultati in questo caso verranno salvati su HDFS.
Per quanto riguarda la profilazione, si può lanciare il comando:

spark-submit --class "spark.ProgettoFinaleBigData.Profiler.ValuesExtractor" --master local[4] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /root/ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar nomeCsv.csv delimiter

per generare i metadati relativi al file nomeCsv.csv. Bisogna specificare il carattere delimitatore del file (, oppure \;).

Questi metadati verranno salvati su mongo (si può accedere alla macchina per un riscontro) nel database metadata, all'interno della collection nomeCsv.

I metadati verranno utilizzati dal modulo ParametricJoin per effettuare il join tra GTD e il dataset nomeCsv.
Il joiner può essere utilizzato con il seguente comando:

spark-submit --class "spark.ProgettoFinaleBigData.PolyglotPersistence.ParametricJoin" --master local[1] --jars /mongo-spark-connector_2.10-2.0.0.jar,/mongo-java-driver-3.4.2.jar /root/ProgettoFinaleBigData-0.0.1-SNAPSHOT.jar hdfs://localhost:9000/input/nomeCsv.csv nomeCsv delimiter

Anche qui nomeCsv del primo parametro è relativo alla directory del file nell'HDFS, va rispecificato il nome per l'estrazione dei metadati da MongoDB e di nuovo il carattere delimitatore utilizzato in precedenza.

Gli altri dataset utilizzati sono reperibili ai seguenti link (vanno scaricati, caricati su HDFS, profilati con ValuesExtractor e quindi joinati con ParametricJoin):

Indicators.csv da https://www.kaggle.com/worldbank/world-development-indicators

nationals.csv da https://www.kaggle.com/umichigan/world-religions

world-energy-use-1960-2012.csv da https://datasource.kapsarc.org/explore/dataset/world-energy-use-1960-2012/information/?disjunctive.country&sort=country

worldbank-gender-statistics.csv da https://datasource.kapsarc.org/explore/dataset/worldbank-gender-statistics/table/

Data.csv da https://www.kaggle.com/theworldbank/world-gender-statistics (probabile sample di WDIData.csv con formattazione leggermente diversa)
