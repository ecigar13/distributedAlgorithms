package floodMax;

import message.*;
import java.util.Set;
import java.util.Queue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Iterator;

public class SlaveThread implements Runnable {

  protected int id;
  private int sno_in_graph;
  private MasterThread masterNode;
  // private SlaveThread parent;
  private int parent = -1;
  int max_uid;
  ArrayList<Integer> neighbours;
  protected boolean newInfo;
  protected String Mtype;
  protected int round;
  Message temp_message_var;
  private int NACK_Count;
  private int ACK_Count;
  public ConcurrentHashMap<Integer, ArrayList<Integer>> slave_children;
  private LinkedBlockingQueue<Message> temp_priority_queue;
  private LinkedBlockingQueue<Message> temp_pq ;
  private ArrayList<Integer>  list_of_children;
  static boolean terminate;
  private LinkedBlockingQueue<Message> temp_msg_pbq;
  protected Map<Integer, Integer> distance = new ConcurrentHashMap<Integer, Integer>();
  protected Map<Integer, SlaveThread> neighbors; 
  //protected Queue<Message> nextRoundMsg = new LinkedBlockingQueue<Message>();
  //protected Queue<Message> thisRoundMsg = new LinkedBlockingQueue<Message>();
  private ArrayList<Messages_in_queue> msgs_in_queues;
  private Messages_in_queue temp_obj;
  private Queue<Integer> final_output;
  // Empty constructor for subclass.
  private int current_node;
  private static ConcurrentHashMap<Integer, Integer> Sno_id_mapping;

  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   * @param sno
   *          = row in the matrix
   */
 
  public SlaveThread(int id, MasterThread masterNode, int sno, ConcurrentHashMap<Integer, ArrayList<Integer>> children, ConcurrentHashMap<Integer, Integer> Sno_id_mapping) 
  {
	   this.id = id;
	   this.max_uid = id;
	   this.masterNode = masterNode;
	   this.sno_in_graph = sno;
	   temp_pq = new LinkedBlockingQueue<>();
	   temp_priority_queue = new LinkedBlockingQueue<>();
	   this.newInfo = true;
	   neighbours = new ArrayList<>();
	   this.round = 0;
	   this.NACK_Count = 0;
	   this.ACK_Count = 0;
	   this.slave_children = children;
	   slave_children = new ConcurrentHashMap<>();
	   list_of_children = new ArrayList<>();
	   temp_msg_pbq = new LinkedBlockingQueue<Message>();
	   msgs_in_queues = new ArrayList<>();
	   final_output = new LinkedList<Integer>();
	   this.Sno_id_mapping = Sno_id_mapping;
	   this.terminate = false;
	   neighbors = new ConcurrentHashMap<Integer, SlaveThread>();
  }	
  
  
  public synchronized void run() 
  {
	  System.out.println("I RAN!!!"+this.sno_in_graph);
	  
	  masterNode.Data_Messages.put(this.sno_in_graph, temp_pq);
	  // find neighbors and store in neighbour Array list
	  for(int temp = 1; temp < masterNode.size; temp++)
	  {
		  //if edge exist in the graph provided
		  if(masterNode.matrix[this.sno_in_graph][temp] == 1)
		  {
			  neighbours.add(temp);
		  }
	  }
	  System.out.println("Neighbors for " + this.sno_in_graph+" : "+ neighbours.size());
	  //check for message in hashmap queue
	  
	  //get hashmap priority queue in a temp queue
	 ArrayList<Integer> temp_array_list = new ArrayList<>();
	  
	  //run until termination condition is encountered
	 
	  while(!terminate)
	  {
		  
		  temp_priority_queue = masterNode.Data_Messages.get(this.sno_in_graph);
		 
			  //newInfo = false;
			  while(!temp_priority_queue.isEmpty())
			  {
				  System.out.println("Size of queue is "+ temp_priority_queue.size()+"Sno thread is "+this.sno_in_graph);
				  temp_message_var = temp_priority_queue.poll();
				  System.out.println("Size of queue is "+ temp_priority_queue.size()+"Sno thread is "+this.sno_in_graph);
				  System.out.println("Message type is "+temp_message_var.getmType()+"Sno thread is "+this.sno_in_graph);
				  	
				  if(temp_message_var.getmType().equals("Terminate"))
				  	{
				  		System.out.println("Inside terminate for slave thread");
				  		Set entrySet = Sno_id_mapping.entrySet();
			  		    // Obtaining an iterator for the entry set
			  		    Iterator<Map.Entry<Integer, Integer>> it = Sno_id_mapping.entrySet().iterator();
			  		    
				  		//output the graph and stop execution
				  		current_node = this.id;
				  		final_output.add(current_node);
				  		System.out.print("The tree formed is " + current_node +"-->");
				  		while(!(final_output.isEmpty()))
				  		{
				  			current_node = final_output.poll();
				  			
				  		
				  		 
				  		    // Iterate through HashMap entries(Key-Value pairs)
				  		    while(it.hasNext())
				  		    {
				  		    	
				  		       Map.Entry<Integer, Integer> m_e = it.next();
				  		       int val = m_e.getValue();
				  		       if(val == current_node)
				  		       {
				  		    	 list_of_children = slave_children.get(m_e.getKey()); 
				  		       }
				  		   }
				  			while(!(list_of_children.isEmpty()))
				  			{
				  				final_output.add(list_of_children.get(0));
				  				list_of_children.remove(0);
				  			}
				  			System.out.print(current_node +"-->");
				  		}
				  		terminate = true;
				  		break;
				  	}
				  	else if(temp_message_var.getmType().equals("Round_Number"))
				  	{
				  		
			  			this.round = temp_message_var.getRound();
			  			System.out.println("Inside else of Round Number, round number is "+this.round+" Sno thread is "+this.sno_in_graph);
			  			if(this.round == 0)
			  			{
			  				//send explore message to all the neighbors
			  				newInfo = true;		
			  			}
			  			
			  			else 
			  				{
					  			//send messages intended for this round
					  			for (int i = 0 ; i < msgs_in_queues.size(); i++)
						  		{
					  				Messages_in_queue transit_message = new Messages_in_queue();
					  				transit_message = msgs_in_queues.get(i);
						  			temp_msg_pbq = masterNode.Data_Messages.get(transit_message.GetId());
						  			temp_msg_pbq.add(transit_message.GetMsg());
						  			masterNode.Data_Messages.put(transit_message.GetId(), temp_msg_pbq);
						  		}
			  				}
			  			//make thread sleep for 2 seconds; local queue should be empty here
			  			for(int i = 0; i < 200000; i++);
				  	}
				  	else if(temp_message_var.getRound()== this.round)
				  	{
				  		System.out.println("If message recieved in this round;inside 3 else");
				  		//increments the round number
				  		if (temp_message_var.getmType().equals("Explore"))
						  {
				  			System.out.println("Inside explore");
							  if(this.max_uid > temp_message_var.getmaxUID())
							  {
								  this.max_uid = temp_message_var.getmaxUID();
								  this.parent = temp_message_var.getSenderId();
								  newInfo = true;
							  }
							  else if (this.max_uid == temp_message_var.getmaxUID())
							  {  
								  //check which parent node has bigger id and choose a parent
								  if(this.parent > temp_message_var.getSenderId())
								  {
									  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"N_ACK"); 
									  temp_obj = new Messages_in_queue(temp_message_var.getSenderId(),temp_msg);
									  //temp_obj.node_s_no = temp_message_var.getSenderId();
									  //temp_obj.msg_to_be_sent = temp_msg;
									  msgs_in_queues.add(temp_obj);
									  
									  
									  //msg_to_be_sent.append(temp_message_var.getSenderId()+":"+temp_msg+";");
								  }
								  else
								  {
									  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"N_ACK"); 
									  temp_obj = new Messages_in_queue(this.parent,temp_msg);
									  //temp_obj.node_s_no = this.parent;
									  //temp_obj.msg_to_be_sent = temp_msg;
									  msgs_in_queues.add(temp_obj);
									
									  this.parent = temp_message_var.getSenderId();
									  
									  
									  //msg_to_be_sent.append(this.parent+":"+temp_msg+";");
									  
								  }
							  }	  
						  }
					  
						  else if(temp_message_var.getmType().equals("N_ACK"))
						  {
							  System.out.println("Inside Nack");
							  this.NACK_Count++;
						  }
						  else if (temp_message_var.getmType().equals("ACK"))
						  {
							  System.out.println("Inside ack");
							  this.ACK_Count++;
							  list_of_children.add(temp_message_var.getSenderId());
							  //add children names to the hash map
							  slave_children.put(this.sno_in_graph, list_of_children);
						  }
					  }
			
			  	// after done processing all the messages, send messages for next round
			  	//send explore messages to all neighbors except parent
			  	if (newInfo)
			  	{
			  		System.out.println("Inside new info");
			  		msgs_in_queues.removeAll(msgs_in_queues);
			  		int neighbour_id;
			  		System.out.println("Size of neighbour list is "+neighbours.size()+"Sno thread is "+this.sno_in_graph);
			  		for(int i = 0; i < neighbours.size(); i++)
			  		{
			  			System.out.println("neighbour id is "+neighbours.get(i));
			  			neighbour_id = neighbours.get(i);
			  			System.out.println("parent is "+this.parent);
			  			if(neighbour_id != this.parent)
			  			{
			  				System.out.println("Sending message to neighbor "+neighbour_id+" from "+this.sno_in_graph);
			  				Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"Explore"); 
			  				temp_obj = new Messages_in_queue(neighbour_id,temp_msg);
			  				//temp_obj.node_s_no = neighbour_id;
							//temp_obj.msg_to_be_sent = temp_msg;
			  				msgs_in_queues.add(temp_obj);
			  				//msg_to_be_sent.append(neighbour_id+":"+temp_msg+";");
			  				
			  			}
			  		}
			  	}
			  	//else divide message from the string into arraylist and send msgs to corresponding nodes
			  	else 
			  	{
			  		System.out.println("Inside else of new info");
			  		//for node leaf
			  		if(NACK_Count == neighbours.size() - 1)
			  		{
			  			  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"ACK"); 
			  			  temp_obj = new Messages_in_queue(this.parent,temp_msg);
//						  temp_obj.node_s_no = this.parent;
//						  temp_obj.msg_to_be_sent = temp_msg;
						  msgs_in_queues.add(temp_obj);
			  		}
			     	// for leader
			  		else if(ACK_Count == neighbours.size() )
			  		{
			  			Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"Leader"); 
			  			temp_obj = new Messages_in_queue(0,temp_msg);
//						  temp_obj.node_s_no = 0;
//						  temp_obj.msg_to_be_sent = temp_msg;
						  msgs_in_queues.add(temp_obj);
			  			
			  		}
			  		//for internal nodes
			  		else if(NACK_Count + ACK_Count == neighbours.size() - 1)
			  		{
			  			  Message temp_msg= new Message(this.sno_in_graph,this.round+1,this.max_uid,"ACK"); 
			  			 temp_obj = new Messages_in_queue(this.parent,temp_msg);
//						  temp_obj.node_s_no = this.parent;
//						  temp_obj.msg_to_be_sent = temp_msg;
						  msgs_in_queues.add(temp_obj);
			  		}
			  		
			  		
			  		/*for (int i = 0 ; i < msgs_in_queues.size(); i++)
			  		{
			  			temp_msg_pbq = masterNode.Data_Messages.get(msgs_in_queues.get(i).GetId());
			  			temp_msg_pbq.add(msgs_in_queues.get(i).GetMsg());
			  			masterNode.Data_Messages.put(msgs_in_queues.get(i).GetId(), temp_msg_pbq);
			  		}*/	    	
			  	} 
			  	
			  	//Message to master about Round Completion
			  	System.out.println("send round done message to master by thread "+this.sno_in_graph);
			  	Message Master_message = new Message(this.id,0,this.round,"Done");
			  	temp_msg_pbq = masterNode.Data_Messages.get(0);
	  			temp_msg_pbq.add(Master_message);
			  	masterNode.Data_Messages.put(0, temp_msg_pbq);
			  	newInfo = false;  	
		  }
		    
	  // while status is FIND DIAMETER, do the find diameter part
	  
	  // set status to FIND FLOODMAX
	  // while status is FIND FLOODMAX, do the find floodmax part
	  
	  // set status to DONE
	  
		 // System.err.println("The thread will now die");
	  }//end of terminate 
	  
  }//end of run
  
  
  class Messages_in_queue
  {
 	int node_s_no;
 	Message msg_to_be_sent;
 	public Messages_in_queue()
 	{}
 	
 	public Messages_in_queue(int sno, Message msg)
 	{
 		this.node_s_no = sno;
 		this.msg_to_be_sent= msg;
 	}
 	public int GetId()
 	{
 		return node_s_no;
 	}
 	public Message GetMsg()
 	{
 		return msg_to_be_sent;
 	}
 	
  };
	  
 // }

 /* public void sendMessage(Message msg) {
    nextRoundMsg.add(msg);
  }

  public enum RoundDone {
    YES, NO;
  }

  protected RoundDone roundFinishStatus = RoundDone.YES;
  protected boolean suspendStatus;


  public SlaveThread(int id, MasterThread masterNode, int sno, ConcurrentHashMap<Integer, ArrayList<Integer>> children,
      ConcurrentHashMap<Integer, Integer> Sno_id_mapping) {
    this.id = id;
    this.max_uid = id;
    this.masterNode = masterNode;
    this.sno_in_graph = sno;
    temp_pq = new LinkedBlockingQueue<>();
    temp_priority_queue = new LinkedBlockingQueue<>();

    neighbours = new ArrayList<>();
    this.round = 0;
    this.NACK_Count = 0;
    this.ACK_Count = 0;
    this.slave_children = children;
    slave_children = new ConcurrentHashMap<>();
    list_of_children = new ArrayList<>();
    temp_msg_pbq = new LinkedBlockingQueue<Message>();
    msgs_in_queues = new ArrayList<>();
    final_output = new LinkedList<Integer>();
    this.Sno_id_mapping = Sno_id_mapping;
  }

  public synchronized void run() {
    System.out.println("I RAN!!!" + this.sno_in_graph);

    masterNode.Data_Messages.put(this.sno_in_graph, temp_pq);
    // find neighbors and store in neighbour Array list
    for (int temp = 1; temp < masterNode.size; temp++) {
      // if edge exist in the graph provided
      if (masterNode.matrix[this.sno_in_graph][temp] == 1) {
        neighbours.add(temp);
      }
    }
    System.out.println("Neighbors for " + this.sno_in_graph + " : " + neighbours.size());
    // check for message in hashmap queue

    // get hashmap priority queue in a temp queue
    ArrayList<Integer> temp_array_list = new ArrayList<>();

    // run until termination condition is encountered

    while (!terminate) {

      temp_priority_queue = masterNode.Data_Messages.get(this.sno_in_graph);

      // newInfo = false;
      while (!temp_priority_queue.isEmpty()) {
        System.out.println("Sno for thread is " + this.sno_in_graph);
        System.out.println("Size of queue is " + temp_priority_queue.size() + "Sno thread is " + this.sno_in_graph);
        temp_message_var = temp_priority_queue.poll();
        System.out.println("Message type is " + temp_message_var.getmType() + "Sno thread is " + this.sno_in_graph);

        if (temp_message_var.getmType().equals("Terminate")) {
          System.out.println("Inside terminate for slave thread");
          Set entrySet = Sno_id_mapping.entrySet();
          // Obtaining an iterator for the entry set
          Iterator<Map.Entry<Integer, Integer>> it = Sno_id_mapping.entrySet().iterator();

          // output the graph and stop execution
          current_node = this.id;
          final_output.add(current_node);
          System.out.print("The tree formed is " + current_node + "-->");
          while (!(final_output.isEmpty())) {
            current_node = final_output.poll();

            // Iterate through HashMap entries(Key-Value pairs)
            while (it.hasNext()) {

              Map.Entry<Integer, Integer> m_e = it.next();
              int val = m_e.getValue();
              if (val == current_node) {
                list_of_children = slave_children.get(m_e.getKey());
              }
            }
            while (!(list_of_children.isEmpty())) {
              final_output.add(list_of_children.get(0));
              list_of_children.remove(0);
            }
            System.out.print(current_node + "-->");
          }
          terminate = true;
          break;
        } else if (temp_message_var.getmType().equals("Round_Number")) {

          this.round = temp_message_var.getRound();
          System.out.println(
              "Inside else of Round Number, round number is " + this.round + " Sno thread is " + this.sno_in_graph);
          if (this.round == 0) {
            // send explore message to all the neighbors
            newInfo = true;
          }

          else {
            // send messages intended for this round
            for (int i = 0; i < msgs_in_queues.size(); i++) {
              Messages_in_queue transit_message = new Messages_in_queue();
              transit_message = msgs_in_queues.get(i);
              temp_msg_pbq = masterNode.Data_Messages.get(transit_message.GetId());
              temp_msg_pbq.add(transit_message.GetMsg());
              masterNode.Data_Messages.put(transit_message.GetId(), temp_msg_pbq);
            }
          }
          // make thread sleep for 2 seconds; local queue should be empty here
          for (int i = 0; i < 200000; i++)
            ;
        } else if (temp_message_var.getRound() == this.round) {
          System.out.println("If message recieved in this round;inside 3 else");
          // increments the round number
          if (temp_message_var.getmType().equals("Explore")) {
            System.out.println("Inside explore");
            if (this.max_uid > temp_message_var.getmaxUID()) {
              this.max_uid = temp_message_var.getmaxUID();
              this.parent = temp_message_var.getSenderId();
              newInfo = true;
            } else if (this.max_uid == temp_message_var.getmaxUID()) {
              // check which parent node has bigger id and choose a parent
              if (this.parent > temp_message_var.getSenderId()) {
                Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "N_ACK");
                temp_obj = new Messages_in_queue(temp_message_var.getSenderId(), temp_msg);
                // temp_obj.node_s_no = temp_message_var.getSenderId();
                // temp_obj.msg_to_be_sent = temp_msg;
                msgs_in_queues.add(temp_obj);

                // msg_to_be_sent.append(temp_message_var.getSenderId()+":"+temp_msg+";");
              } else {
                Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "N_ACK");
                temp_obj.node_s_no = this.parent;
                temp_obj.msg_to_be_sent = temp_msg;
                msgs_in_queues.add(temp_obj);

                this.parent = temp_message_var.getSenderId();

                // msg_to_be_sent.append(this.parent+":"+temp_msg+";");

              }
            }
          }

          else if (temp_message_var.getmType().equals("N_ACK")) {
            System.out.println("Inside Nack");
            this.NACK_Count++;
          } else if (temp_message_var.getmType().equals("ACK")) {
            System.out.println("Inside ack");
            this.ACK_Count++;
            list_of_children.add(temp_message_var.getSenderId());
            // add children names to the hash map
            slave_children.put(this.sno_in_graph, list_of_children);
          }
        }

        // after done processing all the messages, send messages for next round
        // send explore messages to all neighbors except parent
        if (newInfo) {
          System.out.println("Inside new info");
          msgs_in_queues.removeAll(msgs_in_queues);
          int neighbour_id;
          System.out.println("Size of neighbour list is " + neighbours.size() + "Sno thread is " + this.sno_in_graph);
          for (int i = 0; i < neighbours.size(); i++) {
            System.out.println("neighbour id is " + neighbours.get(i));
            neighbour_id = neighbours.get(i);
            System.out.println("parent is " + this.parent);
            if (neighbour_id != this.parent) {
              System.out.println("Sending message to neighbor " + neighbour_id + " from " + this.sno_in_graph);
              Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "Explore");

              temp_obj.node_s_no = neighbour_id;
              temp_obj.msg_to_be_sent = temp_msg;
              msgs_in_queues.add(temp_obj);
              // msg_to_be_sent.append(neighbour_id+":"+temp_msg+";");

            }
          }
        }
        // else divide message from the string into arraylist and send msgs to
        // corresponding nodes
        else {
          System.out.println("Inside else of new info");
          // for node leaf
          if (NACK_Count == neighbours.size() - 1) {
            Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "ACK");
            temp_obj.node_s_no = this.parent;
            temp_obj.msg_to_be_sent = temp_msg;
            msgs_in_queues.add(temp_obj);
          }
          // for leader
          else if (ACK_Count == neighbours.size()) {
            Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "Leader");
            temp_obj.node_s_no = 0;
            temp_obj.msg_to_be_sent = temp_msg;
            msgs_in_queues.add(temp_obj);

          }
          // for internal nodes
          else if (NACK_Count + ACK_Count == neighbours.size() - 1) {
            Message temp_msg = new Message(this.sno_in_graph, this.round + 1, this.max_uid, "ACK");
            temp_obj.node_s_no = this.parent;
            temp_obj.msg_to_be_sent = temp_msg;
            msgs_in_queues.add(temp_obj);
          }

          /*
           * for (int i = 0 ; i < msgs_in_queues.size(); i++) { temp_msg_pbq =
           * masterNode.Data_Messages.get(msgs_in_queues.get(i).GetId());
           * temp_msg_pbq.add(msgs_in_queues.get(i).GetMsg());
           * masterNode.Data_Messages.put(msgs_in_queues.get(i).GetId(), temp_msg_pbq); }
           */
        }

        // Message to master about Round Completion
        System.out.println("send round done message to master");
        Message Master_message = new Message(this.id, 0, this.round, "Done");
        temp_msg_pbq = masterNode.Data_Messages.get(0);
        temp_msg_pbq.add(Master_message);
        masterNode.Data_Messages.put(0, temp_msg_pbq);
        newInfo = false;
      }

      // while status is FIND DIAMETER, do the find diameter part

      // set status to FIND FLOODMAX
      // while status is FIND FLOODMAX, do the find floodmax part

      // set status to DONE

      System.err.println("The thread will now die");
    } // end of terminate

  }// end of run

  class Messages_in_queue {
    int node_s_no;
    Message msg_to_be_sent;

    public Messages_in_queue() {
    }

    public Messages_in_queue(int sno, Message msg) {
      this.node_s_no = sno;
      this.msg_to_be_sent = msg;
    }

    public int GetId() {
      return node_s_no;
    }

    public Message GetMsg() {
      return msg_to_be_sent;
    }

  };

}
