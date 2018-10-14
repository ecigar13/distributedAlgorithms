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

  // max id to be in sync with message class object. Not needed here. Junk value
  protected int masterId = 0;
  protected int size;
  protected int[] neighborArray;
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
   * @param neighbors
   * @param matrix
   */
  public MasterThread(int size, int[] neighbors, int[][] matrix) {
    this.round = 0;
    this.numberOfFinishedThreads = 0;
    this.newInfo = true;
    this.size = size;
    this.neighborArray = neighbors;
    this.matrix = matrix;
    this.name = "Master";

    // put master into concurrent hash map
    MasterThread.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();

    fillLocalMessagesToSend();
  }

  @Override
  public void fillLocalMessagesToSend() {
    System.err.println("Filling localMessagesToSend.");
    localMessagesToSend.put(0, new LinkedBlockingQueue<Message>());
    for (int i : neighborArray) {
      // add signal to start
      localMessagesToSend.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  @Override
  public void run() {
    System.err.println("The Master has started. Size: " + size);

    createThreads();
    fillGlobalQueue();
    sendRoundStartMsg();
    checkGlobalQueueNotEmpty();

    do {
      printMessagesToSendMap(globalIdAndMsgQueueMap);
      sleep();
      // master gets it's own queue from the outside hashmap, with id 0.
      globalIdAndMsgQueueMap.get(0).drainTo(localMessageQueue);

      System.out.println("Master checking its queue. Size of queue is: " + localMessageQueue.size());
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
            try {
              globalIdAndMsgQueueMap.get(tempMsg.getSenderId())
                  .put(new Message(myId, this.round, this.myMaxUid, "Terminate"));
              System.out.println("Telling the master to die.");
              masterMustDie = true;
              return;
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }

          // if a thread says it's done, then master increase the Done count.
          else if ((tempMsg.getmType().equals("Done"))) {
            numberOfFinishedThreads++;
          }
          // all slaves completed the round
          if (numberOfFinishedThreads == size) {

            round++;

            // if all slaves completed the round, master send messages to all nodes to start
            // next round.
            sendRoundStartMsg();
            // Reset Done Count for next round messages
            numberOfFinishedThreads = 0;
          }
        } catch (Exception e) {
          // TODO: handle exception
          e.printStackTrace();
        }

      }
      // sendRoundStartMsg();
      drainToGlobalQueue();
      // run threads
      startAllThreads();
    } while (!masterMustDie);

    System.out.println("Master will now die. ");
  }

  /**
   * Create all nodes/threads, set their names, neighbors, fill the local copy of
   * the global queue.
   */
  public synchronized void createThreads() {
    try {
      for (int row = 0; row < size; row++) {
        SlaveThread t = new SlaveThread(neighborArray[row], this, globalIdAndMsgQueueMap);
        threadList.add(t);

        for (int col = 0; col < size; col++)
          if (matrix[row][col] != 0) {
            t.insertNeighbour(neighborArray[col]);
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

  public synchronized void startAllThreads() {
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
    for (int i : neighborArray) {
      // add signal to start
      localMessageQueue = new LinkedBlockingQueue<Message>();
      globalIdAndMsgQueueMap.put(i, localMessageQueue);
    }
  }

  public void sendRoundStartMsg() {
    for (int i : neighborArray) {

      // don't send msg to itself
      if (i == myId) {
        continue;
      }

      System.out.println("Send Round_Number msg to " + i);
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
    for (int i : neighborArray) {

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
