package floodMax;

import message.Message;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {
  private static final int NULL = 0;
  protected int master_id = 0;
  // protected int sno_in_graph = 0;
  protected int master_round = 0;
  protected boolean newInfo = true;
  // protected MessageType status;
  // max id to be in sync with message class object. Not needed here. Junk value
  protected int max_uid;
  int parent;
  protected String MType;
  private LinkedBlockingQueue<Message> temp_priority_queue;
  private LinkedBlockingQueue<Message> temp_pq;
  private Message temp_Message_obj;
  protected int size;
  private int[] ids;
  protected int[][] matrix;
  // master will put information about the round in this hash map which is
  // accessible to all
  public ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> Data_Messages;
  public ConcurrentHashMap<Integer, Integer> Sno_id_mapping;
  // hash Map for storing children pointers
  public ConcurrentHashMap<Integer, ArrayList<Integer>> children = new ConcurrentHashMap<>();
  private Map<Integer, SlaveThread> slaves = new ConcurrentHashMap<Integer, SlaveThread>();
  private int Done_Count;

  /**
   * Constructor
   * 
   * @param size
   * @param ids
   * @param matrix
   */
  public MasterThread(int size, int[] ids, int[][] matrix,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> map) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.Data_Messages = map;
    this.parent = NULL;
    // put master into concurrent hash map
    // temp_priority_queue = new LinkedBlockingQueue<>();
    temp_priority_queue = new LinkedBlockingQueue<>();
    Done_Count = 0;
    Data_Messages = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
    temp_pq = new LinkedBlockingQueue<>();
    // used for printing the tree at the end
    Sno_id_mapping = new ConcurrentHashMap<Integer, Integer>();
    // setNeighbors();
  }

  @Override
  public void run() 
  {
	 System.out.println(" Inside run of master thread"); 
	  Data_Messages.put(0, temp_pq);
	 
	  for(int i = 1; i < size ; i++)
	  {
		  Sno_id_mapping.put(i, ids[i]);
	  }
	  
	  //ExecutorService executor= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	  try 
	  {
		  for (int i = 1; i < size; i++)   
		  {
			  Thread t = new Thread(new SlaveThread(ids[i], this, i, children,Sno_id_mapping));
			  t.setName("Thread_"+i);
			  t.start();
              //executor.execute(new SlaveThread(ids[i], this, i, children,Sno_id_mapping));                
          }
      }catch(Exception err)
	  {
          err.printStackTrace();
      }
      //executor.shutdown(); // once you are done with ExecutorService
      
     
 	
 
	    //put message generate by the leader into the rows of all the slaves
	    for(int i = 1; i < size; i++)
	    {
	    	 Message msg = new Message(this.master_id,this.master_round, this.max_uid, "Round_Number");
	    	 temp_priority_queue = new LinkedBlockingQueue<>();
	    	 temp_priority_queue.add(msg);
	    	 System.out.println("Sending Round number message to "+i);
	    	 System.out.println("message for "+i+" is : "+temp_priority_queue);
	    	 Data_Messages.put(i, temp_priority_queue);
	    	 
	    	 System.out.println("Message in data message is before " + "i is "+i+ Data_Messages.get(i));
	    	 //temp_priority_queue.remove();
	    	//System.out.println("Message in data message is " + Data_Messages.get(i));
	    	//this.roundFinishStatus = RoundDone.NO;
	    }	  
	  
//	  for(int i = 0; i <size; i++)
//	  {
//		  System.out.println("Master: Size of threads is "+Data_Messages.get(i).size()+" Sno is "+ i);
//	  }
  
	  while(true)
	  {
		
		  temp_priority_queue= Data_Messages.get(0);
		  while(!(temp_priority_queue.isEmpty()))
		  {
			  System.out.println("Master checking its queue");
			  System.out.println("Size of master queue is "+ temp_priority_queue.size());
			  temp_Message_obj = temp_priority_queue.poll();
			  System.out.println("Master checking its queue");
			  System.out.println("Size of master queue is "+ temp_priority_queue.size());
			  //System.out.println(temp_Message_obj.getmType());
			  	if(temp_Message_obj.getmType().equals("Leader"))
			  	{
			  		temp_priority_queue = new LinkedBlockingQueue<>();
			    	Message msg = new Message(this.master_id,this.master_round, this.max_uid, "Terminate");
			    	temp_priority_queue.add(msg);
			    	Data_Messages.put(temp_Message_obj.getSenderId(), temp_priority_queue);
			  	}
			  	
			  	else if ( (temp_Message_obj.getmType().equals("Done")) && (temp_Message_obj.getRound() == this.master_round) )
			  	{
			  		Done_Count++;
			  		System.out.println("Master done count is "+Done_Count);
			  		System.out.print("size - 1 is ");
			  		System.out.println(size-1);
			  		//all slaves completed the round
			  		if (Done_Count == size-1)
			  		{
			  			Done_Count = 0;
			  			this.master_round++;
			  			System.out.println("New round is " +this.master_round);
				    	
				    	for (int i = 1; i< size;i++)
				    	{
				    		Message msg = new Message(this.master_id,this.master_round, this.max_uid, "Round_Number");
					    	temp_priority_queue = new LinkedBlockingQueue<>();
					    	temp_priority_queue.add(msg);
					    	System.out.println("message for "+i+" is : "+temp_priority_queue);
				    		Data_Messages.put(i, temp_priority_queue);
				    		System.out.println("Message in data message is before " + "i is "+i+ Data_Messages.get(i));
				    	}
				    	//Reset Done Count for next round messages
				    	
			  		}
			  	}
		  }
		
	  }
  }
}
	  