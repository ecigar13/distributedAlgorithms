package floodMax;

import message.Message;
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
  protected int masterId = 0;
  // max id to be in sync with message class object. Not needed here. Junk value
  protected int size;
  private int[] ids;
  protected int[][] matrix;
  // master will put information about the round in this hash map which is
  // accessible to all
  public ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  public static ConcurrentHashMap<Integer, Integer> indexToIdMapping;
  // hash Map for storing children pointers
  public ConcurrentHashMap<Integer, ArrayList<Integer>> children = new ConcurrentHashMap<>();
  private int numberOfFinishedThreads;

  /**
   * Constructor
   * 
   * @param size
   * @param ids
   * @param matrix
   */
  public MasterThread(int size, int[] ids, int[][] matrix) {
    this.round = 0;
    this.numberOfFinishedThreads = 0;
    this.newInfo = true;
    this.size = size;
    this.ids = ids;
    this.matrix = matrix;
    this.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
    // put master into concurrent hash map
    localMessageQueue = new LinkedBlockingQueue<Message>();
    globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
    // used for printing the tree at the end
    indexToIdMapping = new ConcurrentHashMap<Integer, Integer>();
  }

  /**
   * Create all nodes/threads, set their names and start them.
   */
  public synchronized void createAndRunThread() {
    try {
      for (int nodeIndex = 1; nodeIndex < size; nodeIndex++) {
        Thread t = new Thread(new SlaveThread(ids[nodeIndex], this, nodeIndex, children, indexToIdMapping));
        t.setName("Thread_" + nodeIndex);
        t.start();
        System.err.println("Created threads. ");
      }
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  @Override
  public void run() {
    System.err.println("The Master has started.");

    for (int i = 1; i < size; i++) {
      indexToIdMapping.put(i, ids[i]);
    }

    createAndRunThread();

    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    synchronized (this) {
      // put message generate by the leader into the rows of all the slaves
      // first create a new queue, then add a msg to the queue, then send queue to
      // global blocking queue
      for (int i = 1; i < size; i++) {
        localMessageQueue = new LinkedBlockingQueue<Message>();
        localMessageQueue.add(new Message(this.masterId, this.round, this.maxUid, "Round_Number"));
        System.out.println("Sending Round number message to " + i);
        System.out.println("message for " + i + " is : " + localMessageQueue);
        globalIdAndMsgQueueMap.put(i, localMessageQueue);
      }

    }

    while (true) {
      // master gets it's own queue from the outside hashmap, with id 0.
      localMessageQueue = globalIdAndMsgQueueMap.get(0);

      while (!(localMessageQueue.isEmpty())) {
        System.out.println("Master checking its queue");
        System.out.println("Size of queue is " + localMessageQueue.size());
        System.out.println();

        tempMsg = localMessageQueue.poll();
        if (tempMsg.getmType().equals("Leader")) {

          // if a node says it's Leader to master, master tells the node to terminate.
          // terminate is a static variable, so all threads will receive the signal.
          Message msg = new Message(this.masterId, this.round, this.maxUid, "Terminate");
          localMessageQueue.add(msg);                                                                       //error might be here
          globalIdAndMsgQueueMap.put(tempMsg.getSenderId(), localMessageQueue);
        }

        // if a thread says it's done, then master increase the Done count.
        else if ((tempMsg.getmType().equals("Done")) && (tempMsg.getRound() == this.round)) {
          numberOfFinishedThreads++;
          // all slaves completed the round
          if (numberOfFinishedThreads == size) {

            this.round++;
            Message msg = new Message(this.masterId, this.round, this.maxUid, "Round_Number");
            localMessageQueue.add(msg);

            // if all slaves completed the round, master send messages to all nodes to start
            // next round.
            for (int i = 1; i < size; i++) {
              globalIdAndMsgQueueMap.put(i, localMessageQueue);
            }
            // Reset Done Count for next round messages
            numberOfFinishedThreads = 0;
          }
        }
      }

    }
  }
}
