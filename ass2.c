#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#define MSG_EXIT 1
#define MSG_PRINT_UNORDERED 2
#define SHIFT_ROW 0
#define SHIFT_COL 1
#define DISP 1

int master_io(MPI_Comm world_comm, MPI_Comm comm);
int slave_io(MPI_Comm world_comm, MPI_Comm comm);
void* ProcessFunc(void *pArg);

pthread_mutex_t g_Mutex = PTHREAD_MUTEX_INITIALIZER;
int g_nslaves = 0;

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

void* ProcessFunc(void *pArg) // Common function prototype
{
	/*char buf[256];
	MPI_Status status;

	while (1) {
		pthread_mutex_lock(&g_Mutex);
		if(g_nslaves <= 0){
			pthread_mutex_unlock(&g_Mutex);
			break;
		}
		MPI_Recv(buf, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status );
		switch (status.MPI_TAG) {
			case MSG_EXIT: 
			{
				g_nslaves--; 
				printf("Thread. g_nslaves: %d\n", g_nslaves);
				break;
			}
			case MSG_PRINT_UNORDERED:
			{
				printf("Thread prints: %s", buf);
				fflush(stdout);
				break;
			}
			default:
			{
				break;
			}
		}
		pthread_mutex_unlock(&g_Mutex);
	}*/
	printf("Thread finished\n");
	fflush(stdout);

	return 0;
}

/* This is the master */
int master_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int size;
	MPI_Comm_size(world_comm, &size );
	g_nslaves = size - 1;
	
	pthread_t tid;
	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, ProcessFunc, NULL); // Create the thread

	char buf[256];
	MPI_Status status;
	/*while (1) {
		pthread_mutex_lock(&g_Mutex);
		if(g_nslaves <= 0){
			pthread_mutex_unlock(&g_Mutex);
			break;
		}
		MPI_Recv(buf, 256, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status );
		switch (status.MPI_TAG) {
			case MSG_EXIT: 
			{
				g_nslaves--; 
				printf("MPI Master Process. g_nslaves: %d\n", g_nslaves);
				break;
			}
			case MSG_PRINT_UNORDERED:
			{
				printf("MPI Master Process prints: %s", buf);
				fflush(stdout);
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
	fflush(stdout);*/
	
	pthread_join(tid, NULL);
	pthread_mutex_destroy(&g_Mutex);
	
    return 0;
}

/* This is the slave */
int slave_io(MPI_Comm world_comm, MPI_Comm comm)
{
	int ndims=2, size, my_rank, reorder, my_cart_rank, ierr, masterSize;
	MPI_Comm comm2D;
	int dims[ndims],coord[ndims];
	int nbr_i_lo, nbr_i_hi;
	int nbr_j_lo, nbr_j_hi;
	int wrap_around[ndims];
	char buf[256];
    
    	MPI_Comm_size(world_comm, &masterSize); // size of the master communicator
  	MPI_Comm_size(comm, &size); // size of the slave communicator
	MPI_Comm_rank(comm, &my_rank);  // rank of the slave communicator
	dims[0]=dims[1]=0;
	
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
	
	for(int a = 0; a < no_iterations; a ++) {
	
	/* PART A (generate random prime numbers and share results with immediate neighbouring processes)
	*/
        MPI_Request send_request[4];
        MPI_Request receive_request[4];
        MPI_Status send_status[4];
        MPI_Status receive_status[4];
	    
	    sleep(my_rank); // puts a sleep because if they all run at once, they'll generate the same number due to the time(NULL)
        unsigned int seed = time(NULL);
	    bool isPrime = false;
	    int randomVal;
	    int k;
	    while (!isPrime) { // run this loop til it generates a prime number
	        int i = rand_r(&seed) % 4 + 1;
	        //printf("Rank: %d, i = %d\n", my_rank, i);
	        if(i > 1){
                int sqrt_i = sqrt(i) + 1;
                for (int j = 2; j <=sqrt_i; j ++) {
                    k = j;    
                    //printf("Rank: %d, k = %d\n", my_rank, k);
                    if(i%j == 0) {
                        break;
                    }
                }
                if(k >= sqrt_i){
                    isPrime = true;
                    randomVal = i;
                }
	        }
	    }
	    MPI_Isend(&randomVal, 1, MPI_INT, nbr_i_lo, 0, comm2D, &send_request[0]);
	    MPI_Isend(&randomVal, 1, MPI_INT, nbr_i_hi, 0, comm2D, &send_request[1]);
	    MPI_Isend(&randomVal, 1, MPI_INT, nbr_j_lo, 0, comm2D, &send_request[2]);
	    MPI_Isend(&randomVal, 1, MPI_INT, nbr_j_hi, 0, comm2D, &send_request[3]);
	    
	    /* initialise variables to store numbers a rank will receive from each of its neighbours.
	    since the grid is not circular, it'll not receive from circular neighbours */
	    int recvValL = -1, recvValR = -1, recvValT = -1, recvValB = -1;
	    MPI_Irecv(&recvValT, 1, MPI_INT, nbr_i_lo, 0, comm2D, &receive_request[0]);
	    MPI_Irecv(&recvValB, 1, MPI_INT, nbr_i_hi, 0, comm2D, &receive_request[1]);
	    MPI_Irecv(&recvValL, 1, MPI_INT, nbr_j_lo, 0, comm2D, &receive_request[2]);
	    MPI_Irecv(&recvValR, 1, MPI_INT, nbr_j_hi, 0, comm2D, &receive_request[3]);
	    
	    MPI_Waitall(4, send_request, send_status);
	    MPI_Waitall(4, receive_request, receive_status);
        printf("Iteration no: %d\n", a);
	    printf("Global rank: %d. Cart rank: %d. Coord: (%d, %d). Random Val: %d. Recv Top: %d. Recv Bottom: %d. Recv Left: %d. Recv Right: %d.\n", my_rank, my_cart_rank, coord[0], coord[1], randomVal, recvValT, recvValB, recvValL, recvValR);
	    
	    
	    char message[200];
	    char file_name[50];
	    if((recvValL == randomVal || recvValL == -1) && (recvValR == randomVal || recvValR == -1) && (recvValT == randomVal || recvValT == -1) && (recvValB == randomVal || recvValB == -1)) {
	        printf("Rank: %d has all matches with its neighbours.\n", my_rank);
	    }
	    else{
	        printf("Rank: %d has not all matches with its neighbours.\n", my_rank);

	    }
    }
	    
    

/*
	printf("Global rank (within slave comm): %d. Cart rank: %d. Coord: (%d, %d).\n", my_rank, my_cart_rank, coord[0], coord[1]);
	fflush(stdout);
*/

	/*sprintf( buf, "Hello from slave %d at Coordinate: (%d, %d)\n", my_rank, coord[0], coord[1]);
	MPI_Send( buf, strlen(buf) + 1, MPI_CHAR, masterSize-1, MSG_PRINT_UNORDERED, world_comm );

	sprintf( buf, "Goodbye from slave %d at Coordinate: (%d, %d)\n", my_rank, coord[0], coord[1]);
	MPI_Send(buf, strlen(buf) + 1, MPI_CHAR, masterSize-1, MSG_PRINT_UNORDERED, world_comm);
	
	sprintf(buf, "Slave %d at Coordinate: (%d, %d) is exiting\n", my_rank, coord[0], coord[1]);
	MPI_Send(buf, strlen(buf) + 1, MPI_CHAR, masterSize-1, MSG_PRINT_UNORDERED, world_comm);
	MPI_Send(buf, 0, MPI_CHAR, masterSize-1, MSG_EXIT, world_comm);*/

    MPI_Comm_free( &comm2D );
	return 0;
}
