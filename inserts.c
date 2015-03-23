//   inserts.c
//   An example of libpq insert methods
//   
//   Copyright 2013 Sterling Jensen <mail@sterlingjensen.com>
//   
//   This program is free software; you can redistribute it and/or modify
//   it under the terms of the GNU General Public License as published by
//   the Free Software Foundation; either version 2 of the License, or
//   at your option) any later version.
//   
//   This program is distributed in the hope that it will be useful,
//   but WITHOUT ANY WARRANTY; without even the implied warranty of
//   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//   GNU General Public License for more details.
//   
//   You should have received a copy of the GNU General Public License
//   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "libpq-fe.h"
#define FIELDS 4
#define ROWS 1000
#define TRIALS 100

struct user {
   char num[10];
   char nam[10];
   char grp[10];
   char ts[20];
};

void eject(PGconn * conn, PGresult * res, char * prefix)
{
   fprintf(stderr, "%s: %s", prefix, PQerrorMessage(conn));
   if(res != NULL){
      PQclear(res);
   }
   PQfinish(conn);
   exit(EXIT_FAILURE);
}
PGconn * conn_unixsocket(void)
{
   const char *conninfo = "dbname = postgres "
                          "user = postgres";
   PGconn   *conn = PQconnectdb(conninfo);
   PGresult *res = NULL;
   if(PQstatus(conn) != CONNECTION_OK)
      eject(conn, res, "Connection to db failed");
   return(conn);
}
PGconn * conn_tcpip(void)
{
   const char *conninfo = "hostaddr = 127.0.0.1 "
                          "dbname = postgres "
                          "user = postgres";
   PGconn   *conn = PQconnectdb(conninfo);
   PGresult *res = NULL;
   if(PQstatus(conn) != CONNECTION_OK)
      eject(conn, res, "Connection to db failed");
   return(conn);

}
void tablesetup(PGconn * conn, PGresult * res)
{
   /* PQexec is blocking, multiple statements return
    * results of only the last statement */
   res = PQexec(conn,
                "CREATE TEMP TABLE tmptable "
                "(num NUMERIC, nam VARCHAR, grp VARCHAR, ts TIMESTAMP);");
   if(PQresultStatus(res) != PGRES_COMMAND_OK){
      fprintf(stderr,"Temp table creation failed: %s",PQerrorMessage(conn));
      PQclear(res);
      PQfinish(conn);
      exit(EXIT_FAILURE);
   }
   PQclear(res);
}
void verify(PGconn * conn, PGresult * res)
{
   res = PQexec(conn, "SELECT * FROM tmptable");
   if(PQresultStatus(res) != PGRES_TUPLES_OK)
      eject(conn, res, "Select failed");
   int nFields = PQnfields(res);
   int nRows = PQntuples(res);
   if(nFields != FIELDS || nRows != ROWS)
      eject(conn, res, "Results do not match input");
   
   /* Print results */
   /*
   int i, j;
   // first, print out the attribute names
   for (i = 0; i < nFields; i++)
      printf("%-15s", PQfname(res, i));
   printf("\n\n");
   // next, print out the rows
   for (i = 0; i < nRows; i++){
      for (j = 0; j < nFields; j++)
         printf("%-15s", PQgetvalue(res, i, j));
      printf("\n");
   }
   */
   PQclear(res);
}

double test_bbbufcopy(PGconn * (*csetup)(void), struct user * df)
{
   struct timeval start, stop;
   /* Setup connection */
   PGconn   * conn = (*csetup)(); // Connect function pointer;
   PGresult * res = NULL;

   tablesetup(conn, res);
   res = PQexec(conn, "COPY tmptable (num,nam,grp,ts) "
                      "FROM STDIN WITH NULL AS ''");
   if(PQresultStatus(res) != PGRES_COPY_IN)
      eject(conn, res, "PGRES_COPY_IN failed");
   PQclear(res);

   gettimeofday(&start, NULL);
   /* Load up the buffer */
   int charcount = (sizeof(df[0])) * ROWS;
   int bufpos = 0;
   char * bbbuf = malloc(charcount);
   for(int i = 0; i < ROWS; i++){
      bufpos += sprintf(bbbuf+bufpos, "%s\t%s\t%s\t%s\n",
         df[i].num,df[i].nam,df[i].grp,df[i].ts);
   }

   /* Push the buffer to postgresql
    * Return -1 error occured, 0 - data not sent, 1 - data sent */ 
   if(PQputCopyData(conn, bbbuf, strlen(bbbuf)) != 1)
      eject(conn, res, "PQputCopyData failed");

   /* Finish COPY
    * Return -1 error occured, 0 - write not ready, 1 - finished */
   if(PQputCopyEnd(conn, NULL) != 1){
      res = PQgetResult(conn); // Must manually pull result
      eject(conn, res, "PQputCopyEnd failed");
   }
   res = PQgetResult(conn);
   PQclear(res);
   gettimeofday(&stop, NULL);

   verify(conn, res); // Verify results
   PQfinish(conn);
   free(bbbuf); // Clear buffer

   double dstart = (start.tv_usec / 1e6) + start.tv_sec;
   double dstop = (stop.tv_usec / 1e6) + stop.tv_sec;
   return(dstop - dstart);
}
double test_incbufcopy(PGconn * (*csetup)(void), struct user * df)
{
   struct timeval start, stop;

   /* Setup connection */
   PGconn   * conn = (*csetup)(); // Connect function pointer;
   PGresult * res = NULL;
   tablesetup(conn, res);
   res = PQexec(conn, "COPY tmptable (num,nam,grp,ts) "
                      "FROM STDIN WITH NULL AS ''");
   if(PQresultStatus(res) != PGRES_COPY_IN)
      eject(conn, res, "PGRES_COPY_IN failed");
   PQclear(res);

   gettimeofday(&start, NULL);
   int charcount = sizeof(df[0]);
   char incbuf[charcount+FIELDS+1]; // include count of string sep and term
   for(int i = 0; i < ROWS; i++){
      sprintf(incbuf, "%s\t%s\t%s\t%s\n",
         df[i].num,df[i].nam,df[i].grp,df[i].ts);
      if(PQputCopyData(conn, incbuf, strlen(incbuf)) != 1)
         eject(conn, res, "PQputCopyData failed");
   }
   if(PQputCopyEnd(conn, NULL) != 1){
      res = PQgetResult(conn); // Must manually pull result
      eject(conn, res, "PQputCopyEnd failed");
   }
   res = PQgetResult(conn);
   PQclear(res);
   gettimeofday(&stop, NULL);

   verify(conn, res); // Verify results
   PQfinish(conn);
   double dstart = (start.tv_usec / 1e6) + start.tv_sec;
   double dstop = (stop.tv_usec / 1e6) + stop.tv_sec;
   return(dstop - dstart);
}
double test_insert(PGconn * (*csetup)(void), struct user * df)
{
   struct timeval start, stop;

   /* Setup connection */
   PGconn   * conn = (*csetup)(); // Connect function pointer;
   PGresult * res = NULL;
   tablesetup(conn, res);
   
   gettimeofday(&start, NULL);
   char template[] = "INSERT INTO tmptable (num,nam,grp,ts) "
                     "VALUES ('%s','%s','%s','%s')";
   char buf[sizeof(df[0])+strlen(template)];
   for(int i = 0; i < ROWS; i++){
      sprintf(buf,template,df[i].num,df[i].nam,df[i].grp,df[i].ts);
      res = PQexec(conn,buf);
      if(PQresultStatus(res) != PGRES_COMMAND_OK){
         eject(conn, res, "INSERT failed");
      }
      PQclear(res);
   }
   gettimeofday(&stop, NULL);
   verify(conn, res);
   PQfinish(conn);
   double dstart = (start.tv_usec / 1e6) + start.tv_sec;
   double dstop = (stop.tv_usec / 1e6) + stop.tv_sec;
   return(dstop - dstart);
}
double test_insertparam(PGconn * (*csetup)(void), struct user * df)
{
   struct timeval start, stop;

   /* Convert struct user to paramValues[] */
   const char * parval[ROWS][FIELDS];
   for(int i = 0; i < ROWS; i++){
      parval[i][0] = (char *)df[i].num;
      parval[i][1] = (char *)df[i].nam;
      parval[i][2] = (char *)df[i].grp;
      parval[i][3] = (char *)df[i].ts;
   }

   /* Setup connection */
   PGconn   * conn = (*csetup)(); // Connect function pointer;
   PGresult * res = NULL;
   tablesetup(conn, res);
   
   gettimeofday(&start, NULL);
   char buf[] = "INSERT INTO tmptable (num,nam,grp,ts) "
                     "VALUES ($1,$2,$3,$4)";
   for(int i = 0; i < ROWS; i++){
      res = PQexecParams(conn,
                         buf,
                         FIELDS,// nParams
                         NULL,  // paramTypes[], NULL force infer type
                         parval[i], // paramValues[]
                         NULL,  // paramLengths[], NULL fine for text type
                         NULL,  // paramFormats[], NULL force text type
                         0      // retultFormat, 0 is text
                         );
      if(PQresultStatus(res) != PGRES_COMMAND_OK)
         eject(conn, res, "INSERT failed");
      PQclear(res);
   }
   gettimeofday(&stop, NULL);
   verify(conn, res);
   PQfinish(conn);
   double dstart = (start.tv_usec / 1e6) + start.tv_sec;
   double dstop = (stop.tv_usec / 1e6) + stop.tv_sec;
   return(dstop - dstart);
}
double test_insertprep(PGconn * (*csetup)(void), struct user * df)
{
   struct timeval start, stop;

   /* Convert struct user to paramValues[] */
   const char * parval[ROWS][FIELDS];
   for(int i = 0; i < ROWS; i++){
      parval[i][0] = (char *)df[i].num;
      parval[i][1] = (char *)df[i].nam;
      parval[i][2] = (char *)df[i].grp;
      parval[i][3] = (char *)df[i].ts;
   }

   /* Setup connection */
   PGconn   * conn = (*csetup)(); // Connect function pointer;
   PGresult * res = NULL;
   tablesetup(conn, res);
   
   gettimeofday(&start, NULL);
   char buf[] = "INSERT INTO tmptable (num,nam,grp,ts) "
                     "VALUES ($1,$2,$3,$4)";
   res = PQprepare(conn,
                   "useradd", // stmtName
                   buf,       // query
                   FIELDS,    // nParams
                   NULL       // paramTypes
                  );
   if(PQresultStatus(res) != PGRES_COMMAND_OK)
      eject(conn, res, "PQprepare fail");
   PQclear(res);
   for(int i = 0; i < ROWS; i++){
      res = PQexecPrepared(conn,
                           "useradd", // stmtName
                           FIELDS,    // nParams
                           parval[i], // paramValues
                           NULL,  // paramLengths, NULL for text type
                           NULL,  // paramFormats
                           0      // resultFormat
                           );
      if(PQresultStatus(res) != PGRES_COMMAND_OK)
         eject(conn, res, "PQprepare fail");
      PQclear(res);
   }
   gettimeofday(&stop, NULL);
   verify(conn, res);
   PQfinish(conn);
   double dstart = (start.tv_usec / 1e6) + start.tv_sec;
   double dstop = (stop.tv_usec / 1e6) + stop.tv_sec;
   return(dstop - dstart);
}

int main(int argc, char **argv)
{
   if(argc || argv){}// NOP to silence warning
   setbuf(stderr, NULL); // disable buffer
   /* Setup test data */
   struct user df[ROWS];
   for(int i = 0; i < ROWS; i++){
      strcpy(df[i].num,"123456789");
      strcpy(df[i].nam,"jsmith");
      strcpy(df[i].grp,"usrgrp");
      strcpy(df[i].ts,"2013-09-25 00:00:01");
   }
   printf("Average insert time of %d rows in %d trials\n",ROWS,TRIALS);
   double avg;

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_bbbufcopy(&conn_unixsocket, df);
   avg = avg / TRIALS;
   printf("%.4f: Big bang buffer copy, unix socket\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_incbufcopy(&conn_unixsocket, df);
   avg = avg / TRIALS;
   printf("%.4f: Incremental buffer copy, unix socket\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insert(&conn_unixsocket, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple insert per row, unix socket\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insertparam(&conn_unixsocket, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple param insert per row, unix socket\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insertprep(&conn_unixsocket, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple execPrep insert per row, unix socket\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_bbbufcopy(&conn_tcpip, df);
   avg = avg / TRIALS;
   printf("%.4f: Big bang buffer copy, tcpip\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_incbufcopy(&conn_tcpip, df);
   avg = avg / TRIALS;
   printf("%.4f: Incremental buffer copy, tcpip\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insert(&conn_tcpip, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple insert per row, tcpip\n",avg);
   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insertparam(&conn_tcpip, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple param insert per row, tcpip\n",avg);

   avg = 0;
   for(int i = 0; i < TRIALS; i++)
      avg += test_insertprep(&conn_tcpip, df);
   avg = avg / TRIALS;
   printf("%.4f: Simple execPrep insert per row, tcpip\n",avg);
}
