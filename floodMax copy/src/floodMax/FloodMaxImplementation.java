package floodMax;
import message.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class FloodMaxImplementation implements Runnable {
  private int size;
  private int[] ids;
  private int[][] matrix;
  private int leader;
  ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> common_map;

  /*public int getLeader() 
  {
    return leader;
  }

  public void setLeader(int leader) 
  {
    this.leader = leader;
  }
*/
  public FloodMaxImplementation(int size, int[] ids, int[][] matrix, ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> map) 
  {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.common_map = map;
  }

  @Override
  public void run() 
  {

    // Master thread create slave threads and run them
    //MasterThread masterNode = new MasterThread(this.size, this.ids, this.matrix,this.common_map);
    Thread thread = new Thread(new MasterThread(this.size, this.ids, this.matrix,this.common_map));
    thread.start();

    // Output the leaderId
   // System.out.println("Leader is: " + masterNode.getLeaderId());

  }

}
