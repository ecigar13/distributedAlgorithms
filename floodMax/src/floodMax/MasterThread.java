package floodMax;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import message.Message;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {
  public static ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;

  protected int masterId = 0;
  protected int size;
  protected int[] slaveArray;
  protected int[][] matrix;

  protected boolean masterMustDie = false;

  // master will put information about the round in this hash map which is
  // accessible to all
  // hash Map for storing children pointers
  private int numberOfFinishedThreads;
  private ArrayList<SlaveThread> threadList = new ArrayList<SlaveThread>();
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  /**
   * Constructor
   * 
   * @param size
   * @param slaveArray
   * @param matrix
   */
  public MasterThread(int size, int[] slaveArray, int[][] matrix) {
    this.round = 1;
    this.numberOfFinishedThreads = 0;
    this.newInfo = true;
    this.size = size;
    this.slaveArray = slaveArray;
    this.matrix = matrix;
    this.name = "Master";
    this.myId = 0;

    // put master into concurrent hash map
    MasterThread.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();

    fillLocalMessagesToSend();
  }

  @Override
  public void fillLocalMessagesToSend() {
    // System.err.println("Filling localMessagesToSend.");
    localMessagesToSend.put(myId, new LinkedBlockingQueue<Message>());
    for (int i : slaveArray) {
      // add signal to start
      localMessagesToSend.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  @Override
  public void run() {
    System.out.println("The Master has started. Size: " + size);

    createThreads();
    fillGlobalQueue();
    sendRoundStartMsg();
    // checkGlobalQueueNotEmpty();

    do {
      // master gets it's own queue from the outside hashmap, with id 0.
      // sleep();
      System.out
          .println("Master checking its queue. Size of queue is: " + localMessageQueue.size() + " round " + round);
      globalIdAndMsgQueueMap.get(0).drainTo(localMessageQueue);
      // printMessagesToSendMap(globalIdAndMsgQueueMap);

      while (!(localMessageQueue.isEmpty())) {
        try {
          Message tempMsg = localMessageQueue.take();
          System.out.println(tempMsg);

          if (tempMsg.getmType().equalsIgnoreCase("Leader")) {

            // if a node says it's Leader to master, master tells the node to terminate.
            // terminate is a static variable, so all threads will receive the signal.

            // empty the remaining msg in master's q.
            localMessageQueue.clear();

            // send terminate msg back to the sender.
            globalIdAndMsgQueueMap.get(tempMsg.getSenderId()).put(new Message(myId, round, myMaxUid, "Terminate"));
            System.err
                .println("-------Telling the master to die. Leader is: " + tempMsg.getSenderId() + " round " + round);
            masterMustDie = true;
            myMaxUid = tempMsg.getSenderId();
            killAll();
          }

          // if a thread says it's done, then master increase the Done count.
          else if ((tempMsg.getmType().equals("Done"))) {
            numberOfFinishedThreads++;
          }
          // all slaves completed the round
          if (numberOfFinishedThreads == size) {
            round++;

            // if all slaves completed the round, master tells nodes to start next round
            sendRoundStartMsg();
            // Reset Done Count for next round messages
            numberOfFinishedThreads = 0;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }

      }
      drainToGlobalQueue();
      System.out.println("Starting threads. ");
      startAllThreads();

      // wait for threads to finish
      while (globalIdAndMsgQueueMap.get(myId).size() < slaveArray.length) {
        System.out.println(globalIdAndMsgQueueMap.get(myId).size());
        sleep();
      }
    } while (!masterMustDie);

    // printTree();
    printNackAckTree();
    System.out.println("Master will now die. MaxUid " + myMaxUid + " round " + round);

  }

  /**
   * Print the nackAck set after the algorithm is done.
   */
  public void printTree() {
    System.out.println("\n\nPrinting the tree.");
    System.out.println("Parent <--- myId ---> myChildren (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.getMyParent() + "<------" + t.getId() + "------>");
      for (int i : t.getNeighborSet()) {
        if (i != t.getMyParent()) {
          System.out.print(i + " ");
        }
      }
      System.out.println();
    }
  }

  /**
   * Print the neighbor set after the algorithm is done.
   */
  public void printNackAckTree() {
    System.out.println("\n\nPrinting the tree.");
    System.out.println("Parent <--- myId ---> myChildren (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.getMyParent() + "<------" + t.getId() + "------>");
      for (int i : t.getNackReceived()) {
        if (i != t.getMyParent()) {
          System.out.print(i + " ");
        }
      }

      for (int i : t.getAckReceived()) {
        if (i != t.getMyParent()) {
          System.out.print(i + " ");
        }
      }
      System.out.println();
    }
  }

  /**
   * Create all nodes/threads, set their names, neighbors, fill the local copy of
   * the global queue.
   */
  public void createThreads() {
    try {
      for (int row = 0; row < size; row++) {
        SlaveThread t = new SlaveThread(slaveArray[row], this, globalIdAndMsgQueueMap);
        threadList.add(t);

        for (int col = 0; col < size; col++)
          if (matrix[row][col] != 0) {
            t.insertNeighbour(slaveArray[col]);
            // System.out.println(ids[i]);
          }
        // find index

        // create a similar structure to the master's global queue. Used to store msg
        // before draining all to global q.
        t.fillLocalMessagesToSend();
      }

      System.err.println("Created threads. ");
    } catch (Exception err) {
      err.printStackTrace();
    }
  }

  public void startAllThreads() {
    for (SlaveThread t : threadList) {
      t.run();
    }
  }

  /**
   * First step of master thread: fill the global queue.
   */
  public void fillGlobalQueue() {
    System.err.println("Filling global queue.");
    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    for (int i : slaveArray) {
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      globalIdAndMsgQueueMap.put(i, localMessageQueue);
    }
  }

  public void sendRoundStartMsg() {
    for (int i : slaveArray) {

      // don't send msg to itself
      if (i == myId) {
        continue;
      }

      System.err.println("Send Round_Number msg to " + i);
      try {
        Message temp = new Message(myId, round, myId, "Round_Number");
        localMessagesToSend.get(i).put(temp);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void checkGlobalQueueNotEmpty() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> p : globalIdAndMsgQueueMap.entrySet()) {
      System.out.println(p.getKey() + " " + p.getValue());
    }
  }

  public void killAll() {
    for (int i : slaveArray) {

      // don't send msg to itself
      if (i == myId) {
        continue;
      }

      System.out.println("Send Terminate msg to " + i);
      try {
        Message temp = new Message(myId, round, myId, "Terminate");
        localMessagesToSend.get(i).put(temp);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
