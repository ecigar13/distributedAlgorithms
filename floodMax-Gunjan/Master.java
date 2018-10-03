import java.io.*;
import java.util.IOException;
import java.util.Exception;

public class Master extends Thread
{
    int latest_round;
    Queue<message>  master_queue; 
    message temp_msg;
    int count = 0;
    Master(int Master_UID, Storage str,int round, int n_o_n)
    {
	latest_round = round;
	master_queue = new Queue<message>();
    }
    public run()
    {
       
	if(latest_round == 0)
	    {
		for (int nodes = 1; nodes<n_o_n; nodes++)
		    {
			/*data.node = nodes;
			msg.UID = Master_UID;
			msg.max_UID=null;
			msg.round = latest_round ;*/

			//put data into message variable
			message msg = new message(Master_UID, null, latest_round,"Round_Information");
			//push data into the hash map object and object into the hashmap
			data data = new data( msg);
			map.put(nodes, data);
		    }
	    }

	while (true)
	    {
		master_queue= map.get(Master_UID); 
		temp_msg = master_queue.pop();
		
		if(temp_msg.round() == latest_round)
		    {
			if ( temp_msg.type.equals("Round_Done") )            //additional check
			    {
				count = count + 1;
			    }

			if(count == n_o_n)
			    {
				latest_round = latest_round + 1;
				for (int nodes = 1; nodes<n_o_n; nodes++)
				    {
					//put data into message variable
					message msg = new message(Master_UID, null, latest_round,"Round_Information");
					//push data into the hash map object and object into the hashmap                                                                                                                    
					data data = new data( msg);
					map.put(nodes, data);
				    }

			    }
		    }
	    }
    }
}