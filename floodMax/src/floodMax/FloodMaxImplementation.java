package floodMax;

import message.Message;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class FloodMaxImplementation implements Runnable {
  private int size;
  private int[] ids;
  private int[][] matrix;
  ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> idAndMsgQueueMap;

  /*
   * public int getLeader() { return leader; }
   * 
   * public void setLeader(int leader) { this.leader = leader; }
   */
  public FloodMaxImplementation(int size, int[] ids, int[][] matrix,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> idAndMsgQueueMap) {
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.idAndMsgQueueMap = idAndMsgQueueMap;
  }

  @Override
  public void run() {

    // Master thread create slave threads and run them
    MasterThread thread = new MasterThread(this.size, ids, this.matrix);
    thread.setName("thread_0");
    thread.run();

    // Output the leaderId
    // System.out.println("Leader is: " + masterNode.getLeaderId());

  }

}
