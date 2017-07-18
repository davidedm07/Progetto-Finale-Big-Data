# Progetto-Finale-Big-Data
Progetto Finale del corso di Big Data 2016/2017. 
Lo scopo del progetto è quello di creare un sistema poliglotta in grado di eseguire analisi su database di diversa natura.
Per testare il sistema, basta clonare il progetto ed avviare lo script create_dockers.sh:

  bash create_dockers.sh
  
Lo script effettua il provisioning delle macchine Docker, carica sul container mongoDB il database GTS, 
carica i jar di connessione e del progetto Java sul container hadoop.

A questo punto, bisogna accedere al terminale della macchina mongoDB lanciando:
  
  docker exec -it mongoDB bash
  
Da qui dobbiamo eseguire il comando:

  bash create_database.sh 
 
che avrà l'effetto di caricare il GTD su mongoDB (nel database dbTerr e collection attacks)
