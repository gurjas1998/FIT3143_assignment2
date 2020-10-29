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
#define DISP 1 // Displacement == 1, so refers to immediate neighbour adjacent to current rank

#define TEMP_REQUEST 1
#define TEMP_CHECK 2

int nrows, ncols;

// Function declarations
int master_io(MPI_Comm world_comm, MPI_Comm comm);
int slave_io(MPI_Comm world_comm, MPI_Comm comm);
void* ProcessFunc(void *pArg);
int random_number(int min_num, int max_num);

pthread_mutex_t g_Mutex = PTHREAD_MUTEX_INITIALIZER; // Creating mutex
int g_nslaves = 0;
int finish = 0; // If 1, then we terminate the program
bool skip = false;
int masterSize;
int *temp = NULL; // Pointer to address containing value of type int
time_t *timestamp = NULL; // ^ type time_t, used to store system time values

/* Creation of struct which allows node to send multiple bits of information
   to master node all at once */
struct valuestruct { 
	int temp; // Node's own temp
	int leftTemp; // The following are temp messages from adjacent nodes
	int rightTemp;
	int topTemp;
	int bottomTemp;
	int matches; // Number of nodes that have accepted similarity in temperature 
};



int main(int argc, char **argv) // Specify grid size via command line arguments, eg "3 2" for a 3x2 grid
                                // Int argc refers to number of arguments, argv are the actual arguments supplied 
{
    int rank, size;
    
    MPI_Comm new_comm;
    MPI_Init(&argc, &argv); // Initialise MPI execution environment
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    // If user specifies grid size then create grid as follows
    if (argc == 3) {
		nrows = atoi (argv[1]);
		ncols = atoi (argv[2]);
		// If user specifies grid size that doesn't match number of processes - 1 then error
		if((nrows*ncols) != size-1) {
			if(rank ==0) printf("ERROR: nrows*ncols = %d * %d = %d != %d nodes\n", nrows, ncols, nrows*ncols, size-1);
			MPI_Finalize(); 
			return 0;
		}
	} 
	// If user doesn't specify grid size, i think the program will throw an error "divide-by-zero"
	else {
		nrows=ncols=0;
	}
    
    // Split up processes into 1 master and size-1 slaves
    MPI_Comm_split(MPI_COMM_WORLD, rank == size-1, 0, &new_comm);
    if (rank == size-1) master_io(MPI_COMM_WORLD, new_comm);
    else slave_io( MPI_COMM_WORLD, new_comm );
    MPI_Finalize();
    return 0;
}

/* Function to determine random temperature
   Also used to determine random coordinate (rank) for thread */
int random_number(int min_num, int max_num)
    {
        int result = 0, low_num = 0, hi_num = 0;
       
        if (min_num < max_num)
        {
            low_num = min_num;
            hi_num = max_num + 1; // Include max_num in output
        } else {
            low_num = max_num + 1; // Include max_num in output
            hi_num = min_num;
        }

        result = (rand() % (hi_num - low_num)) + low_num;
        return result;
    }


// Thread function within master node
void* ProcessFunc(void *pArg)
{
        
    while(1){
        // When finish == 1, then thread exits, otherwise keep running
        if(finish == 1){
            break;
        }
        // If master is writing to the file, then don't update the satellite value
        if(skip){
            continue;  
            /* The 'continue' means to NOT proceed with following code for current iteration
               ie. if skip is true, then keep checking skip until it's false then
               execute following code.
               Variable skip is seen in master node but since the thread shares
               the same rank, it can also see it? */
        }
        int lastTemp;
        int sensorPos;
        sensorPos = random_number(0, masterSize-2); // Choosing some random coordinate/rank
        lastTemp = temp[sensorPos]; // Getting reference for latest temperature recorded by satellite at that coordinate
        temp[sensorPos] = random_number(lastTemp-10,lastTemp+10); // Record new temperature for that coordinate
        timestamp[sensorPos] = time(NULL); // Specify at what time this temperature was taken
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
	// MPI_Comm_size determines size of group associated with a communicator
	// Why is it being called twice here? 
	MPI_Comm_size(world_comm, &size);
	MPI_Comm_size(world_comm, &masterSize);
	
	int iteration = 0;
	
	struct valuestruct values;
	// The following is necessary to create the MPI struct
	MPI_Datatype Valuetype;
	MPI_Datatype type[6] = {MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_INT};
	int blocklen[6] = {1,1,1,1,1,1}; // Len of list elements?
	MPI_Aint disp[6]; // To store address of parameters

    /* MPI_Get_address gets the address of a location in memory
       Here we are getting addresses of struct values and storing in disp array
       so that we may manipulate them */
	MPI_Get_address(&values.temp, &disp[0]);
	MPI_Get_address(&values.leftTemp, &disp[1]);
	MPI_Get_address(&values.rightTemp, &disp[2]);
	MPI_Get_address(&values.topTemp, &disp[3]);
	MPI_Get_address(&values.bottomTemp, &disp[4]);
	MPI_Get_address(&values.matches, &disp[5]);

	// Make relative
	disp[5]=disp[5]-disp[0];
	disp[4]=disp[4]-disp[0];
	disp[3]=disp[3]-disp[0];
	disp[2]=disp[2]-disp[0];
	disp[1]=disp[1]-disp[0];
	disp[0]=0;

	// Create MPI struct
	MPI_Type_create_struct(6, blocklen, disp, type, &Valuetype);
    
    // A datatype object has to be committed before it can be used in a communication
	MPI_Type_commit(&Valuetype); 
	
	int m,n;
	MPI_Request end_request;
	MPI_Status end_status;
	//int flag = 0 ;
	int end_message = 1;
	FILE *pInfile; 
	m = nrows;
	n = ncols;
	
	
	int *totalMessages = NULL;
	g_nslaves = size - 1;
    srand((unsigned)time(NULL)+((size-1)*masterSize));
	temp = (int*)malloc(size * sizeof(int));
	totalMessages = (int*)malloc(size * sizeof(int)); // Number of times a process has sent a request to master
	timestamp = (time_t*)malloc(size * sizeof(time_t)); // Last time thread updated temp value from the infrared
	
	/* The following is to initialise temperatures to be used in thread (satellite).
	   We're also initialising values for arrays timestamp and totalMessages */
	int low_num = 70, hi_num = 100;
	for(int i = 0; i < size-1; i++){
		temp[i] = (rand() % (hi_num - low_num)) + low_num;
		timestamp[i] = time(NULL);
		totalMessages[i]=0;
	}
	pthread_t tid;
	pthread_mutex_init(&g_Mutex, NULL);
	pthread_create(&tid, 0, ProcessFunc, NULL); // Create the thread
	
	pInfile = fopen("log.txt","w");
	
	time_t secs = 10; // 2 minutes (can be retrieved from user's input)

    time_t startTime = time(NULL);
    while ((time(NULL) - startTime) < secs){
		if(finish == 1){
            break;
        }
        
        int temperature;
        MPI_Request request;
        MPI_Status status;
        MPI_Status receive_status;
        int flag = 0;
        int fire_rank;
        int alertType;
        time_t node_to_base_start = time(NULL);
        time_t node_to_base_end; 
		MPI_Irecv(&fire_rank, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &request);
		while(!flag){
		    /* MPI_Test checks whether request has been received or not
		       sets flag to true if yes */
		    flag = 1;
		    //MPI_Test(&request, &flag, &receive_status); 
		    if((time(NULL) - startTime) > secs){
		        break;
		    }
            if(flag){
                node_to_base_end = time(NULL);
            }
		}
		if(!flag){
		    break;
		}
		iteration++;
		totalMessages[fire_rank]++;
		time_t alertTime = time(NULL);
		
		skip = true;
		MPI_Recv(&values, 6, Valuetype, MPI_ANY_SOURCE, MPI_ANY_TAG, world_comm, &status);
		temperature = values.temp;
		if(temperature <= temp[fire_rank] + 5 && temperature >= temp[fire_rank] - 5){
		    /* If the received temperature is within +/- 5 of 'temp seen by satellite', then
		       it is a real alert */
		    alertType = 1;
		}
		else {
		    // Otherwise, it is a false alert
		    alertType = 0;
		}
		
		// Here we prepare writing to the file
		double node_to_base_time = difftime(node_to_base_end, node_to_base_start);
		time_t current_time;
		// c_time() returns a string representing the localtime based on the argument timer
		char* c_time_string;
		current_time = time(NULL);
		c_time_string = ctime(&current_time);
		fprintf(pInfile, "--------------------------------\n");
		fprintf(pInfile, "Iteration : %d\n",iteration);
		fprintf(pInfile, "Logged Time : %s\n",c_time_string);
		c_time_string = ctime(&alertTime);
		fprintf(pInfile, "Alert Reported Time : %s\n",c_time_string);
		if(alertType == 1){
		    fprintf(pInfile, "Alert Type: True\n");
		}
		else{
		    fprintf(pInfile, "Alert Type: False\n");
		}
		fprintf(pInfile, "\nReporting Node\tCoord\tTemp\n");
		fprintf(pInfile, "%d\t(%d,%d)\t%d\n",fire_rank,fire_rank/n,fire_rank%n,values.temp);
		fprintf(pInfile, "\nAdjacent Node\tCoord\tTemp\n");
		if((fire_rank%n)-1 >=0)
		    fprintf(pInfile, "%d\t(%d,%d)\t%d\n",fire_rank-1,fire_rank/n,(fire_rank%n)-1,values.leftTemp);
		if((fire_rank%n)+1 <n)
		    fprintf(pInfile, "%d\t(%d,%d)\t%d\n",fire_rank+1,fire_rank/n,(fire_rank%n)+1,values.rightTemp);
		if((fire_rank/n)-1 >=0)
		    fprintf(pInfile, "%d\t(%d,%d)\t%d\n",fire_rank-n,(fire_rank/n)-1,fire_rank%n,values.topTemp);
		if((fire_rank/n)+1 <m )
		    fprintf(pInfile, "%d\t(%d,%d)\t%d\n",fire_rank+n,(fire_rank/n)+1,fire_rank%n,values.bottomTemp);
		c_time_string = ctime(&timestamp[fire_rank]);
		fprintf(pInfile, "\nInfrared Satellite Reporting Time (Celsius) : %s\n",c_time_string);
		fprintf(pInfile, "Infrared Satellite Reporting (Celsius) : %d\n",temp[fire_rank]);
		fprintf(pInfile, "Infrared Satellite Reporting Coord : (%d,%d)\n",fire_rank/n,fire_rank%n);
		
		fprintf(pInfile, "\nTotal Messages send between reporting node and base station : %d\n",totalMessages[fire_rank]);
		fprintf(pInfile, "Number of adjacent matches to reporting node : %d\n",values.matches);
		fprintf(pInfile, "Communication time between requesting node and base station: %f\n", node_to_base_time);
		fprintf(pInfile, "--------------------------------\n");
		skip = false;
	}
	
	
	finish = 1;
	fclose(pInfile); // close the file
	pInfile = NULL;
	
	// Master sends broadcast using world_comm to slaves saying it's time to terminate
	MPI_Ibcast(&end_message, 1, MPI_INT, masterSize-1, world_comm, &end_request);
	
	MPI_Wait(&end_request, &end_status);
	printf("MPI Master Process finished\n");
	fflush(stdout);
	
	pthread_join(tid, NULL);
	pthread_mutex_destroy(&g_Mutex);
	
    return 0;
}

/* This is the slave */
int slave_io(MPI_Comm world_comm, MPI_Comm comm)
{
    // Preparing to create grid 
	int ndims=2, size, my_rank, reorder, my_cart_rank, ierr;
	MPI_Comm comm2D;
	int dims[ndims], coord[ndims];
	int nbr_i_lo, nbr_i_hi; // These represent adjacent nodes
	int nbr_j_lo, nbr_j_hi;
	int wrap_around[ndims];
    int temp = -1;
    MPI_Comm_size(world_comm, &masterSize); // size of the master communicator
  	MPI_Comm_size(comm, &size); // size of the slave communicator
	MPI_Comm_rank(comm, &my_rank);  // rank of the slave communicator
	dims[0]=dims[1]=0;
	
	// The following is same as in Master node
	struct valuestruct values;
	MPI_Datatype Valuetype;
	MPI_Datatype type[6] = {MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_INT,MPI_INT};
	int blocklen[6] = {1,1,1,1,1,1};
	MPI_Aint disp[6];

	MPI_Get_address(&values.temp, &disp[0]);
	MPI_Get_address(&values.leftTemp, &disp[1]);
	MPI_Get_address(&values.rightTemp, &disp[2]);
	MPI_Get_address(&values.topTemp, &disp[3]);
	MPI_Get_address(&values.bottomTemp, &disp[4]);
	MPI_Get_address(&values.matches, &disp[5]);

	//Make relative
	disp[5]=disp[5]-disp[0];
	disp[4]=disp[4]-disp[0];
	disp[3]=disp[3]-disp[0];
	disp[2]=disp[2]-disp[0];
	disp[1]=disp[1]-disp[0];
	disp[0]=0;

	// Create MPI struct
	MPI_Type_create_struct(6, blocklen, disp, type, &Valuetype);
	MPI_Type_commit(&Valuetype);
	
	//MPI_Bcast(&m, 1, MPI_INT, masterSize-1, world_comm);
	//MPI_Bcast(&n, 1, MPI_INT, masterSize-1, world_comm);
	dims[0]=nrows;
	dims[1]=ncols;
	
	//sleep(my_rank);
	int low_num = 70, hi_num = 100;
	
	srand((unsigned)time(NULL)+((my_rank+1)*size));
	temp = (rand() % (hi_num - low_num)) + low_num;
    printf("temp %d\n",temp);
    
    
	MPI_Dims_create(size, ndims, dims);
    	if(my_rank == 0){
    		printf("Slave Rank: %d. Comm Size: %d: Grid Dimension = [%d x %d] \n",my_rank,size,dims[0],dims[1]);
    	}

	/* Create cartesian mapping */
	wrap_around[0] = 0;
	wrap_around[1] = 0; // Periodic shift is .false.
	reorder = 0;
	ierr = 0;
	ierr = MPI_Cart_create(comm, ndims, dims, wrap_around, reorder, &comm2D);
	if(ierr != 0) printf("ERROR[%d] creating CART\n",ierr);
	
	/* Find my coordinates in the cartesian communicator group */
	MPI_Cart_coords(comm2D, my_rank, ndims, coord); // Coordinated is returned into the coord array
	/* Use my cartesian coordinates to find my rank in cartesian group*/
	MPI_Cart_rank(comm2D, coord, &my_cart_rank);
	
	
	// Neighbour nodes
	MPI_Cart_shift( comm2D, SHIFT_ROW, DISP, &nbr_i_lo, &nbr_i_hi );
	MPI_Cart_shift( comm2D, SHIFT_COL, DISP, &nbr_j_lo, &nbr_j_hi );
	
	int count = 0;
	
	// End the reading
	MPI_Request end_request;
	MPI_Status end_status;
	int flag = 0;
	int end_message;
	MPI_Ibcast(&end_message, 1, MPI_INT, masterSize-1, world_comm, &end_request);
	
	while(!flag) {
	    if (!flag){
            MPI_Test(&end_request, &flag, &end_status);
	    }
	    if (flag){
            break;
	    }
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
	    
        /* Initialise variables to store numbers a rank will receive from each of its neighbours.
        since the grid is not circular, it'll not receive from circular neighbours */
	    int recvValL = -1, recvValR = -1, recvValT = -1, recvValB = -1;
	    MPI_Irecv(&recvValT, 1, MPI_INT, nbr_i_lo, 0, comm2D, &receive_request[0]);
	    MPI_Irecv(&recvValB, 1, MPI_INT, nbr_i_hi, 0, comm2D, &receive_request[1]);
	    MPI_Irecv(&recvValL, 1, MPI_INT, nbr_j_lo, 0, comm2D, &receive_request[2]);
	    MPI_Irecv(&recvValR, 1, MPI_INT, nbr_j_hi, 0, comm2D, &receive_request[3]);
	    
	    MPI_Waitall(4, send_request, send_status);
	    MPI_Waitall(4, receive_request, receive_status);

	    if(temp >= 70){
	        // Check if adjacent nodes have similar temperature
	        int inRange = 0;
	        if(recvValT >= temp -5 && recvValT <= temp+5){
	            inRange++;
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
	        
	        // If at least 2 adjacent nodes have a similar temperature, send info to Master
	        if(inRange >=2){
	            printf("Rank : %d, temp > 80, sending to master\n", my_rank);
	            MPI_Request sendRequest;
	            MPI_Status sendStatus;
	            MPI_Isend(&my_rank, 1, MPI_INT, masterSize-1, TEMP_CHECK, world_comm, &sendRequest);
	            MPI_Wait(&sendRequest,&sendStatus);
	            printf("Success sending to master\n");
	            
	            // Record node temperature and neighbour nodes' temps to send to master in values struct
	            values.temp = temp;
	            values.leftTemp = recvValL;
	            values.rightTemp = recvValR;
	            values.topTemp = recvValT;
	            values.bottomTemp = recvValB;
	            values.matches = inRange;
	            MPI_Send(&values, 6, Valuetype, masterSize-1, TEMP_CHECK, world_comm);
	        }
        }
        low_num = temp - 15, hi_num = temp + 15;       
        temp = (rand() % (hi_num - low_num)) + low_num;   
        
        // MPI_Barrier blocks until all processes in comm have reached this routine
        MPI_Barrier(comm);
         
        if (!flag){
            MPI_Test(&end_request, &flag, &end_status);
	    }
	    if (flag){
            break;
	    }
        sleep(2);
    }
    printf("rank : %d End\n", my_rank);
    MPI_Comm_free( &comm2D );
	return 0;
}


/*
Is not specifying grid size supposed to make the program terminate?

In master_io, why is MPI_Comm_size called twice using world_comm but stored in two different variables? around line 141

What's the point in making addresses relative? Why not just store addresses as they are? line 162

Is line 213 necessary? "if(finish == 1)" because finish = 1 isn't called until outside of the while loop.

Have a look at node_to_base_time again, not sure if that's done properly. I think it should be time 
since node sends the request through? That requires modifying the struct to send the time value over,
not sure if that's possible to have an MPI datatype for time though

Is line 435 meant to be "if(temp >= 80)"? instead of 70?
*/
