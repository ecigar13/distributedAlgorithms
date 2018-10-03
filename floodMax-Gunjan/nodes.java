import java.io.*;
import java.util.IOException;
import java.util.Exception;


public class nodes extends Thread
{
    
    int slave_round;
    Queue<message> slave_queue;
    message temp_message ;
    int count = 0;
    int ack_count = 0;
    int nack_count = 0;
    int My_UID;
    int total_nodes;
    int parent;
    ArrayList<Integer> children;
    ArrayList<Integer> neighbours;
    int max_id ; 
    boolean updated_info;
    ArrayList<String> data_to_be_forwarded;
    int number_of_neighbours = 0;
    //int message return count

    nodes(int uid, Storage str, int[][] adj_matrix, int n_o_n)
    {
	My_UID = uid;
	total_nodes = n_o_n; 
	slave_queue = new Queue<message>();
	children = new ArrayList<Integer>();
	neighbours = new ArrayList<Integer>();
	max_id = My_UID;
	data_to_be_forwarded = new ArrayList<>(); 
    }

    public run()
    {
	for (int i = 0; i <= total_nodes; i++)
	    {
		if(adj_matrix[My_UID][i] == 1)
		    {
			neighbours.push(i);
			number_of_neighbours = number_of_neighbours + 1;
		    }
	    }
	//Initiating sending for first round
	slave_queue = map.get(My_UID);
	temp_message = slave.queue.pop();
	if(temp_message.type().equals("Round Information"))
	    {
		//round should be 0
		slave_round = temp_message.round();
		
		//send uid to all the neighbours

	    }


	while (true)
	    {
		slave_queue = map.get(My_UID);
		//temp_message = slave_queue.pop();
		while (!slave_queue.isEmpty())
		    {
			temp_message = slave_queue.pop();
			if(temp_message.type.equals("Round_Information") )
			    {
				slave_round = temp_message.round();
				String msg[] = data_to_be_forwarded.split("/");

				for(int i = 0; i < msg.length ; i++)
				    {
					String message_split[] = msg[i].split(":");
					//put data in hash map for all nodes
					str.put(Integer.parseint(message_split[0]),message.split[1]);
				    }	
				
			    }
			else if( (temp_message.type().equals("explore")) && (temp_message.round() == slave_round) )
			    {
				if(max_id < temp_message.max_UID() )
				    {
					max_id = temp_message.max_UID();
					parent = temp_message.uid();
					updated_info = "true";
				    }
				else
				    {
					updated_info = "false";
				    }
			    }
			else if( (temp_message.type().equals("ack")) && (temp_message.round() == slave_round) )
			    {
				ack_count = ack_count+1;
			    }
			else if ( (temp_message.type().equals("nack")) &&(temp_message.round() == slave_round) )
			    {
				nack_count = nack_count+1;
			    }
		    }

		if(updated_info == "true")
		    {
			//send message to all the neighbours except parent
		    }
		if(ack = neighbours)
		    {
			status = "leader";
			//send message to all master
		    }
		if(nack+ack = neighbours - 1)
		    {
			//send ack to parent

		    }
		
		
	    }
			
    }


}