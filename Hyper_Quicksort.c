/**********************************************************************
**********************************************************************/

#include <time.h>
#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <math.h>
#include <unistd.h>

#define MASTER 0        /* task ID of master task */
#define PIVOT_TRANSFER 9999
#define HIGHER_CHUNK 9998
#define LOWER_CHUNK 9997
#define COUNT_TRANSFER 9996

// function to swap elements
void swap(int *a, int *b) {
  int t = *a;
  *a = *b;
  *b = t;
}

// function to find the partition position
int partition(int array[], int low, int high) {
  
  // select the rightmost element as pivot
  int pivot = array[high];
  
  // pointer for greater element
  int i = (low - 1);
  int j;

  // traverse each element of the array
  // compare them with the pivot
  for (j = low; j < high; j++) {
    if (array[j] <= pivot) {
        
      // if element smaller than pivot is found
      // swap it with the greater element pointed by i
      i++;
      
      // swap element at i with element at j
      swap(&array[i], &array[j]);
    }
  }

  // swap the pivot element with the greater element at i
  swap(&array[i + 1], &array[high]);
  
  // return the partition point
  return (i + 1);
}

void quickSort(int array[], int low, int high) {
  if (low < high) {
    
    // find the pivot element such that
    // elements smaller than pivot are on left of pivot
    // elements greater than pivot are on right of pivot
    int pi = partition(array, low, high);
    
    // recursive call on the left of pivot
    quickSort(array, low, pi - 1);
    
    // recursive call on the right of pivot
    quickSort(array, pi + 1, high);
  }
}

int main (int argc, char *argv[])
{

    int processid,             /* process ID */
        numprocesses,           /* number of processes */
        i,j,k,
        d;
    int p = numprocesses;
    int n = 4096;
    int initial_pivot_processID = 0;
    double startTime, elapsedTime;   //variable to calculate thee execution time
    MPI_Status status;           // MPI status variable initialisation
    int pivot;
    time_t t1;
    srand ( (unsigned) time (&t1)); // pass the srand() parameter  

    /* Obtain number of tasks and task ID */
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD,&numprocesses);

    startTime = MPI_Wtime();
    MPI_Comm_rank(MPI_COMM_WORLD,&processid);

    p = numprocesses;
    d = log(p)/log(2);
    int arr[n];

    for (i = 0; i < n; i++)
    {
        if(i<n/p)
        {
            arr[i] = rand()%(92*(processid+1));
        }
        else
        {
            arr[i] = -1;
        }
    }

    if(processid == initial_pivot_processID)
    {
        pivot = arr[rand()%((n/p)-2)+1];
    }
    MPI_Bcast(&pivot,1, MPI_INT, 0, MPI_COMM_WORLD);

    for (i = 1; i <= d; i++)
    {
        //finding the length
        j = 0;
        while(arr[j]!=-1)
        {
            j++;
        }

        //partition
        int l=0;
        int m=j-1;
        int temp,temp_pivot;
        temp_pivot = pivot;
        while(l<m){
            while(arr[l]<=temp_pivot && l<m)
            l++;
            while(arr[m]>temp_pivot)
            m--;
            if(l<m){
                temp=arr[l];
                arr[l]=arr[m];
                arr[m]=temp;
            }
        }
        temp=temp_pivot;
        temp_pivot=arr[m];
        arr[m]=temp;

        //setting b1-b2 start end variable
        int b1_start=0;
        int b1_end;
        l=-1;
        while(arr[l+1]<=pivot && arr[l+1]!=-1)
        {
            l++;
        }
        
        b1_end = l;
        if(l==-1)
            b1_end=0;
        int b2_start = b1_end+1;
        int b2_end = j-1;

        // mapping for each iteration
        int mapping[p];
        int num,bit;
        int temp_pid;
        for (k = 0; k < p; k++)
        {
            num = k;
            bit = d-i+1;
            num=num>>(bit-1);
    	    if((num&1)==0)
	        {
                temp_pid = (k+pow(2,d-i));
                mapping[k] = temp_pid;
                mapping[temp_pid] = k;
    	    }   
        }

        // transfer the data
        int send_higher_arr[n];
        int send_lower_arr[n];
        int z,ind,recv_ind;

        int recv_lower_arr[n];
        int recv_higher_arr[n];
        int temp_arr[n];

        num = processid;
        bit = d-i+1;
        num=num>>(bit-1);
    	if((num&1)==0)
	    {
            //mpisend higher chunks
            ind=0;
            for (z = b2_start; z <= b2_end; z++)
            {
                send_higher_arr[ind++]=arr[z];
            }
            for (z = ind; z < n; z++)
            {
                send_higher_arr[z]=-1;
            }
            MPI_Send(send_higher_arr,n, MPI_INT,mapping[processid], HIGHER_CHUNK, MPI_COMM_WORLD);
            //mpirecv lower chunks
            MPI_Recv(recv_lower_arr,n, MPI_INT, MPI_ANY_SOURCE, LOWER_CHUNK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            //merge
            ind=0;
            for(z = b1_start; z <= b1_end; z++)
            {
                temp_arr[ind++]=arr[z];
            }
            recv_ind=0;
            while(recv_lower_arr[recv_ind]!=-1)
            {
                temp_arr[ind++] = recv_lower_arr[recv_ind++];
            }
            while(ind<n)
            {
                temp_arr[ind++]=-1;
            }
    	}
        else
        {
            //mpirecv higher chunks
            MPI_Recv(recv_higher_arr,n, MPI_INT, MPI_ANY_SOURCE, HIGHER_CHUNK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //mpisend lower chunks
            ind=0;
            for (z = b1_start; z <= b1_end; z++)
            {
                send_lower_arr[ind++]=arr[z];
            }
            for (z = ind; z < n; z++)
            {
                send_lower_arr[z]=-1;
            }
            MPI_Send(&send_lower_arr[0],n, MPI_INT,mapping[processid], LOWER_CHUNK, MPI_COMM_WORLD);

            //merge
            ind=0;
            for(z = b2_start; z <= b2_end; z++)
            {
                temp_arr[ind++]=arr[z];
            }
            recv_ind=0;
            while(recv_higher_arr[recv_ind]!=-1)
            {
                temp_arr[ind++] = recv_higher_arr[recv_ind++];
            }
            while(ind<n)
            {
                temp_arr[ind++]=-1;
            }
        }
            
        //merge into our main arr at last
        for (z = 0; z < n; z++)
        {
            arr[z]=temp_arr[z];
        }

        //select pivot now and broadcast to the group
        if(i!=d)
        {
            int grp = pow(2,i);
            int start_grp = 0;
            int end_grp,grp_pid;
            int total_process_in_grp = p/grp;
            int parent=-1;
            for (k = 0; k < p; k=k+total_process_in_grp)
            {
                if(processid==k)
                {
                    pivot = arr[0];
                    for (grp_pid = k+1; grp_pid < k+total_process_in_grp; grp_pid++)
                    {
                        //mpisend send pivot to other grp_pid
                        MPI_Send(&pivot, 1, MPI_INT, grp_pid, PIVOT_TRANSFER, MPI_COMM_WORLD);
                    }
                    parent=k;
                    break;
                }
            }
            if (parent==-1)
            {
                //recieve pivot for those process
                MPI_Recv(&pivot, 1, MPI_INT, MPI_ANY_SOURCE, PIVOT_TRANSFER, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
        }
    }
    int final_end = 0;
    while(arr[final_end]!=-1)
        final_end++;
    quickSort(arr,0,final_end-1);

    int process_element_count;
    int counts[p],z;

    if(processid!=MASTER)
    {
        MPI_Send(&final_end, 1, MPI_INT, 0, COUNT_TRANSFER, MPI_COMM_WORLD);
    }
    else
    {
        counts[MASTER]=final_end;
        for(z=1;z<p;z++)
        {
            MPI_Recv(&process_element_count, 1, MPI_INT, MPI_ANY_SOURCE, COUNT_TRANSFER, MPI_COMM_WORLD, &status);
            int recv_id = status.MPI_SOURCE;
            counts[recv_id] = process_element_count;   
        }
    }

    int displacement[p];
    if(processid==MASTER)
    {
        int from_end=0;
        int count_ind=0;
        int count_sum = 0;
        while(count_ind<p)
        {
            displacement[from_end++] = count_sum;
            count_sum+=counts[count_ind++];
        }
    }

    int final_array[n];
    if(processid==MASTER)
    {
        MPI_Gatherv(arr,final_end,MPI_INT,final_array,counts,displacement,MPI_INT,MASTER,MPI_COMM_WORLD);
    }
    else
    {
        MPI_Gatherv(arr,final_end,MPI_INT,NULL,NULL,NULL,MPI_INT,MASTER,MPI_COMM_WORLD);
    }

    if(processid==MASTER)
    {

        printf("\n\nFINAL\n");
        for(i = 0; i < n; i++)
        {
            printf("%d ", final_array[i]);
        }
        printf("\n");        
    }

    elapsedTime = MPI_Wtime ( ) - startTime;
    MPI_Finalize ( );

    //Process 0 prints a termination message.
    if ( processid == 0 )
    {
        printf("%f\n", elapsedTime);
    }

    return 0;
}