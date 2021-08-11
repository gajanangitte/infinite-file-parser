#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include<time.h>

#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif
#ifndef D_LARGEFILE_SOURCE
#define D_LARGEFILE_SOURCE 1
#endif
#ifndef D_FILE_OFFSET_BITS
#define D_FILE_OFFSET_BITS 64
#endif

// Contrary to the given statement that line/record size is variable, this
// we will consider a maximum limit for the record size to exist as name, epoch and userid are
// definite in size 
#define        MAXLINELENGTH    256 // Max record size

// 512 MB : The size of each buffer read as we have
// 1 GB RAM, other processes and system tasks may take upto 
// half of the RAM space, i.e. for 4GB log file: max reads ~ 8(Eight)
#define        BUFSIZE      512*1024*1024 

FILE *f_rest; 

int read_from_file_open(char *filename)
{
int fd;

fd = open(filename, O_RDONLY|O_LARGEFILE);
   if (fd == -1)
    {
       printf("\nFile Open Unsuccessful\n");
       exit (0);;
    }
struct stat sb;

    if(fstat(fd, &sb) != -1) {
        printf("Size successfully retrieved: %ld\n", sb.st_size);
    }
size_t size = sb.st_size;
long *buffer=(long*) malloc(size * sizeof(long));
off_t chunk=0;
lseek(fd,0,SEEK_SET);
// printf("\nCurrent Position%d\n",lseek(fd,size,SEEK_SET));
while ( chunk < size )
  {
   printf ("the size of chunk read is  %ld\n",chunk);
   size_t readnow;
   readnow=read(fd,((char *)buffer)+chunk,1024);
    
    printf("%s\n", (char *)buffer);
   if (readnow < 0 )
     {
        printf("\nRead Unsuccessful\n");
        free (buffer);
        close (fd);
        return 0;
     }

   chunk=chunk+readnow;
  }

printf("\nRead Successful\n");

free(buffer);
close(fd);
return 1;

}

size_t min(size_t a, size_t b) {
  return a < b ? a : b;
}

long recalc_position (char *buf, int begin) {
  int i = begin;
  while(buf[i] != "\n" && i >= 0) {
    i--;
  }
  if(i > 0)
    return i+1;

}

long long bytesread;
char buf[BUFSIZE];
long long  sizeLeftover=0;
int  taskFinished = 0;
long long pos = 0;

long long calc_entries(char* buf, long long size, int taskFinished, int *idx_counter) {
  char temp[strlen(buf) + 1];
  strcpy(temp, buf);
  char *token = strtok(temp, "/n");
  long long num_entries = 0;

   /* walk through other tokens */
   while( token != NULL ) {
      // printf( " %s\n", token );
      num_entries++;
      token = strtok(NULL, "/n");
   }
  
  return num_entries;
}

void search(char *buf, long long size, long long *idx_counter, time_t start_time, time_t end_time, char* name , long long start_idx, long long end_idx) {
  
  char temp[strlen(buf) + 1];
  strcpy(temp, buf);
  char *c, *d;
  char *token = strtok_r(temp, "/n", &c);

  // WE CAN USE BINARY SEARCH LIKE JUMPING SYSTEM 
  // TO DO: use binary search like mechanism to highlight the searches

  
   /* walk through other tokens lying before our index*/
   while( token != NULL && *idx_counter < start_idx ) {

      (*idx_counter)++;
      token = strtok_r(NULL, "/n", &c);
   }

   while( token != NULL && *idx_counter <= end_idx) {

      char* time = strtok_r(token, ",", &d);
      char* u_name = strtok_r(time, ",", &d);
      time_t cur = strtoul(time, NULL, 0);
      
      if(cur >= start_time && cur <= end_time && name == u_name) {
          // we can store this result in a file as well but currently we are just priniting it out
        fprintf(f_rest, "Time: %s, User: %s", time, u_name);
      }
      
      (*idx_counter)++;
      token = strtok(NULL, "/n");


   }


}
          

void find(char *start_time, char *end_time, char* name, long long start_idx, long long end_idx) {

    f_rest = fopen("result.txt", "w");
    // log filename goes here
    FILE *handler = fopen("file.txt","rb");
    if( !handler )
    {
      printf("Error getting log file\n");
      return 0;
    }

    // CONVERT GIVEN TIME INPUTS INTO EPOCH TIME FORMAT
    struct tm s_tm, e_tm;
    char* st = strptime(start_time, "%Y-%m-%d %H:%M", &s_tm);
    char* ed = strptime(end_time, "%Y-%m-%d %H:%M", &e_tm);
    time_t srt_tm = mktime(&s_tm), end_tm = mktime(&e_tm);

    int idx_counter = 0;
    do {

        bytesread = fread(buf+sizeLeftover, 1, sizeof(buf)-1-sizeLeftover, handler);
        
        if (bytesread <= 0) {
            printf("\n\nLoop completed");
            taskFinished = 1;
            bytesread  = 0;
            break;
        }

        buf[bytesread+sizeLeftover] = 0; 
        long long num_entries = calc_entries( buf, bytesread+sizeLeftover, &taskFinished, idx_counter);
        
        pos = recalc_position( buf, bytesread+sizeLeftover);
        if(pos < 1) {
          taskFinished=1;
          pos = 0;
        }

        if(idx_counter <= start_idx && idx_counter + num_entries >= start_idx) {
          // BEGIN THE SEARCH
          search(buf, bytesread+pos, &idx_counter, srt_tm, end_tm, name ,start_idx, end_idx);
          idx_counter += num_entries;
        }
        else if( idx_counter <= end_idx && idx_counter + num_entries >= end_idx  ) {
          search(buf, bytesread+pos, &idx_counter, srt_tm, end_tm, name ,start_idx, end_idx);
          idx_counter += num_entries;
        }
        else
          idx_counter+=num_entries;

        sizeLeftover = min(bytesread+sizeLeftover-pos,sizeof(buf) - MAXLINELENGTH);
        
        if(sizeLeftover < 0)
          sizeLeftover = 0;
        
        // for an incomplete record/log we will add it to our next buffer read
        // continued from where we left our last processed record
        // current leftover and together complete a full readable lines
        if(pos!=0 && sizeLeftover != 0)
          memmove(buf,buf+pos,sizeLeftover);

    }
    while(taskFinished == 0);

    fclose(handler);

}