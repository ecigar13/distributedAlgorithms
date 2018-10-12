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
  public static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  // max id to be in sync with message class object. Not needed here. Junk value
  protected int masterId = 0;
  protected int size;
  protected int[] ids;
  protected int[][] matrix;

  // master will put information about the round in this hash map which is
  // accessible to all
  // hash Map for storing children pointers
  private int numberOfFinishedThreads;
  private ArrayList<SlaveThread> threadList = new ArrayList<SlaveThread>();

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
    // put master into concurrent hash map
    MasterThread.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
  }

  @Override
  public void run() {
    System.err.println("The Master has started.");

    createThreads();
    setNeighbors();
    fillGlobalQueue();
    startAllThreads();

    while (true) {
      // master gets it's own queue from the outside hashmap, with id 0.
      localMessageQueue = globalIdAndMsgQueueMap.get(0);

      while (!(localMessageQueue.isEmpty())) {
        System.out.println("Master checking its queue");
        System.out.println("Size of queue is " + localMessageQueue.size());
        System.out.println();

        Message tempMsg = localMessageQueue.poll();
        if (tempMsg.getmType().equals("Leader")) {

          // if a node says it's Leader to master, master tells the node to terminate.
          // terminate is a static variable, so all threads will receive the signal.
          Message msg = new Message(this.masterId, this.round, this.maxUid, "Terminate");
          localMessageQueue.add(msg); // error might be here
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

  /**
   * Create all nodes/threads, set their names and start them.
   */
  public synchronized void createThreads() {
    try {
      for (int nodeIndex = 1; nodeIndex < size; nodeIndex++) {
        SlaveThread t = new SlaveThread(ids[nodeIndex], this, globalIdAndMsgQueueMap);
        threadList.add(t);
        t.setName("Thread_" + nodeIndex);

        System.err.println("Created threads. ");
      }
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  public synchronized void startAllThreads() {
    for (Thread t : threadList) {
      t.start();
    }
  }

  /**
   * Implement. Set an array of neighbor id -> use it to find queue in global
   * queue.
   */
  public synchronized void setNeighbors() {
    for (SlaveThread t : threadList) {
      for (int i = 1; i < size; i++) {
        if (ids[i] != 0) {
          t.insertNeighbour(ids[i]);
          System.out.println(ids[i]);
        }
      }
    }
  }

  /**
   * First step of master thread: send round 0 start signal.
   */
  public void fillGlobalQueue() {
    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    for (int i = 1; i < size; i++) {
      System.out.println("Sending Round_Number message to " + ids[i]);
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      localMessageQueue.add(new Message(this.masterId, this.round, this.maxUid, "Round_Number"));
      globalIdAndMsgQueueMap.put(ids[i], localMessageQueue);

    }
  }
}
