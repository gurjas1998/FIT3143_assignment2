#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <math.h>
#include <stdbool.h>

#define MSG_EXIT 1
#define MSG_PRINT_UNORDERED 2
#define SHIFT_ROW 0
#define SHIFT_COL 1
#define DISP 1

#define TEMP_REQUEST 1
#define TEMP_CHECK 2

int master_io(MPI_Comm world_comm, MPI_Comm comm);
int slave_io(MPI_Comm world_comm, MPI_Comm comm);
void* ProcessFunc(void *pArg);
int random_number(int min_num, int max_num, int a);

pthread_mutex_t g_Mutex = PTHREAD_MUTEX_INITIALIZER;
int g_nslaves = 0;
int finish = 0;
int masterSize;
int *temp = NULL;
int *timestamp = NULL;
//unsigned int seed = time(NULL);

int main(int argc, char **argv)
{
    int rank, size;
    
    MPI_Comm new_comm;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    
    MPI_Comm_split( MPI_COMM_WORLD,rank == size-1, 0, &new_comm);
    if (rank == size-1) 
	master_io( MPI_COMM_WORLD, new_comm );
    else
	slave_io( MPI_COMM_WORLD, new_comm );
    MPI_Finalize();
    return 0;
}

int random_number(int min_num, int max_num, int a)
    {
        int result = 0, low_num = 0, hi_num = 0;
       
        if (min_num < max_num)
        {
            low_num = min_num;
            hi_num = max_num + 1; // include max_num in output
        } else {
            low_num = max_num + 1; // include max_num in output
            hi_num = min_num;
        }

        srand(a+2);
        result = (rand() % (hi_num - low_num)) + low_num;
        return result;
    }

void* ProcessFunc(void *pArg) // Common function prototype
{
    int sensorPos = -1;
    int counter = 0;
    
    /*while(1){
		counter++;
        if(finish == 1){
            break;
        }
        sensorPos = random_number(0,masterSize-2);
        if(temp[sensorPos] == -1){
            temp[sensorPos] = random_number(30,100);
            counter++;
        }
        if(counter >= masterSize-2){
            break;
        }
    }*/
    
    counter = 0;
    while(counter <=10){
        counter++;
        if(finish == 1){
            break;
        }
        int lastTemp;
        sensorPos = random_number(0,masterSize-2,masterSize);
        lastTemp = temp[sensorPos];
        int a = random_number(lastTemp-10,lastTemp+20,masterSize);
        temp[sensorPos] = 75;
        sleep(2);
    }
	
	printf("Thread finished\n");
	fflush(stdout);

	return 0;
}

/* This is the master */
int master_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int size;
	MPI_Comm_size(world_comm, &size );
	MPI_Comm_size(world_comm, &masterSize);
	g_nslaves = size - 1;
    srand((unsigned)time(NULL)+((size-1)*masterSize));
	temp = (int*)malloc(size * sizeof(int));
	//seed = (size+1) * time(NULL));
	timestamp = (int*)malloc(size * sizeof(int));
	
	for(int i = 0; i < size-1; i++)
		temp[i] = 75;
	
	pthread_t tid;
	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, ProcessFunc, NULL); // Create the thread
	MPI_Status status;
	while (finish ==0) {
		
		if(finish == 1){
            break;
        }
        
        int temperature;
		MPI_Recv(&temperature, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status );
		printf("recieved request!!!!!!!!!!");
		pthread_mutex_lock(&g_Mutex);
		switch (status.MPI_TAG) {
			case TEMP_CHECK: 
			{
			    if(temperature <= temp[status.MPI_SOURCE] +5 && temperature >= temp[status.MPI_SOURCE] -5){
			        printf("radar : %d \n", temp[status.MPI_SOURCE]);
			        printf("FIRE !!!!!!!!!!");
			    } 
			    else{
			        printf("FALSE FIRE :)");
			    }
			    break;
			}
			case MSG_EXIT:
			{
				finish = 1;
				break;
			}
			default:
			{
				break;
			}
		}
		pthread_mutex_unlock(&g_Mutex);
	}
	printf("MPI Master Process finished\n");
	fflush(stdout);
	
	pthread_join(tid, NULL);
	pthread_mutex_destroy(&g_Mutex);
	
    return 0;
}

/* This is the slave */
int slave_io(MPI_Comm world_comm, MPI_Comm comm)
{
    
	int ndims=2, size, my_rank, reorder, my_cart_rank, ierr;
	MPI_Comm comm2D;
	int dims[ndims],coord[ndims];
	int nbr_i_lo, nbr_i_hi;
	int nbr_j_lo, nbr_j_hi;
	int wrap_around[ndims];
    int temp = -1;
    //seed =  time(NULL) % (my_rank+2);
    
    
	//temp = random_number(30,100);
	
    MPI_Comm_size(world_comm, &masterSize); // size of the master communicator
  	MPI_Comm_size(comm, &size); // size of the slave communicator
	MPI_Comm_rank(comm, &my_rank);  // rank of the slave communicator
	dims[0]=dims[1]=0;
	//sleep(my_rank);
	int low_num = 30, hi_num = 100;
	
        //srand((unsigned)time(NULL)+((my_rank+1)*size));        
        //temp = (rand() % (hi_num - low_num)) + low_num;
    sleep(my_rank);
	srand((unsigned)time(NULL)+((my_rank+1)*size));
	temp = (rand() % (hi_num - low_num)) + low_num;
    printf("temp %d\n",temp);
    low_num = temp -15;
    hi_num = temp+15;
    temp = (rand() % (hi_num - low_num)) + low_num;  
    printf("temp %d\n",temp);
	MPI_Dims_create(size, ndims, dims);
    	if(my_rank==0)
		printf("Slave Rank: %d. Comm Size: %d: Grid Dimension = [%d x %d] \n",my_rank,size,dims[0],dims[1]);

    	/* create cartesian mapping */
	wrap_around[0] = 0;
	wrap_around[1] = 0; /* periodic shift is .false. */
	reorder = 0;
	ierr =0;
	ierr = MPI_Cart_create(comm, ndims, dims, wrap_around, reorder, &comm2D);
	if(ierr != 0) printf("ERROR[%d] creating CART\n",ierr);
	
	/* find my coordinates in the cartesian communicator group */
	MPI_Cart_coords(comm2D, my_rank, ndims, coord); // coordinated is returned into the coord array
	/* use my cartesian coordinates to find my rank in cartesian group*/
	MPI_Cart_rank(comm2D, coord, &my_cart_rank);
	
	
	//neighbour 
	MPI_Cart_shift( comm2D, SHIFT_ROW, DISP, &nbr_i_lo, &nbr_i_hi );
	MPI_Cart_shift( comm2D, SHIFT_COL, DISP, &nbr_j_lo, &nbr_j_hi );
	
	int count = 0;
	while(count < 5) {
	    
	    
	
	/* PART A (generate random prime numbers and share results with immediate neighbouring processes)
	*/  
	    count ++;
	    printf("rank : %d , temp : %d temp\n", my_rank , temp);
	    
	    MPI_Request send_request[4];
        MPI_Request receive_request[4];
        MPI_Status send_status[4];
        MPI_Status receive_status[4];
            
        MPI_Isend(&temp, 1, MPI_INT, nbr_i_lo, 0, comm2D, &send_request[0]);
	    MPI_Isend(&temp, 1, MPI_INT, nbr_i_hi, 0, comm2D, &send_request[1]);
	    MPI_Isend(&temp, 1, MPI_INT, nbr_j_lo, 0, comm2D, &send_request[2]);
	    MPI_Isend(&temp, 1, MPI_INT, nbr_j_hi, 0, comm2D, &send_request[3]);
	    
	        /* initialise variables to store numbers a rank will receive from each of its neighbours.
	        since the grid is not circular, it'll not receive from circular neighbours */
	    int recvValL = -1, recvValR = -1, recvValT = -1, recvValB = -1;
	    MPI_Irecv(&recvValT, 1, MPI_INT, nbr_i_lo, 0, comm2D, &receive_request[0]);
	    MPI_Irecv(&recvValB, 1, MPI_INT, nbr_i_hi, 0, comm2D, &receive_request[1]);
	    MPI_Irecv(&recvValL, 1, MPI_INT, nbr_j_lo, 0, comm2D, &receive_request[2]);
	    MPI_Irecv(&recvValR, 1, MPI_INT, nbr_j_hi, 0, comm2D, &receive_request[3]);
	    
	    MPI_Waitall(4, send_request, send_status);
	    MPI_Waitall(4, receive_request, receive_status);
	    if(temp >= 70){
	        
	        
	        int inRange = 0;
	        if(recvValT >= temp -5 && recvValT <= temp+5){
	            inRange++;
	            //printf("rank : %d , temp : %d temp  top: %d\n", my_rank , temp,nbr_i_lo);
	        }
	        if(recvValB >= temp -5 && recvValB <= temp+5){
	            inRange++;
	        }
	        if(recvValL >= temp -5 && recvValL <= temp+5){
	            inRange++;
	        }
	        if(recvValR >= temp -5 && recvValR <= temp+5){
	            inRange++;
	        }
	        
	        if(inRange >=2){
	            printf("rank : %d , temp >80 sending to master\n", my_rank);
	            MPI_Send(&temp, 1, MPI_INT, masterSize-1, TEMP_CHECK, world_comm);
	            // send message to master
	        }
        }
        low_num = temp -15, hi_num = temp+15;
        //srand((unsigned)time(NULL)+((my_rank+1)*size));        
        temp = (rand() % (hi_num - low_num)) + low_num;     
        sleep(2);
    }
    printf("rank : %d End\n", my_rank);
    if(my_rank == 0){
    MPI_Send(&temp, 1, MPI_INT, masterSize-1, MSG_EXIT, world_comm);
    }
    MPI_Comm_free( &comm2D );
	return 0;
}
































