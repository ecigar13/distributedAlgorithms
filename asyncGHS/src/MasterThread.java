
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MasterNode is a special case of a SlaveNode.
 * 
 * @author khoa
 *
 */
public class MasterThread extends SlaveThread {

  protected int masterId = 0;
  protected int size;
  protected int[] slaveArray;
  protected double[][] matrix;

  protected int noMwoeCount = 0;
  protected boolean masterMustDie = false;

  private int numberOfFinishedThreads;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  private ArrayList<SlaveThread> threadList = new ArrayList<SlaveThread>();

  protected HashSet<Integer> leaderSet = new HashSet<>();

  /**
   * Constructor
   * 
   * @param size
   * @param slaveArray
   * @param matrix
   */
  public MasterThread(int size, int[] slaveArray, double[][] matrix) {
    this.round = 1;
    this.numberOfFinishedThreads = 0;
    this.size = size;
    this.slaveArray = slaveArray;
    this.matrix = matrix;
    this.name = "Master";
    this.id = 0;
    this.level = 0;
    this.coreLink = null;

    this.globalIdAndMsgQueueMap = new ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>>();
    initGlobalIdAndMsgQueueMap();
  }

  /**
   * First step of master thread: fill the global queue.
   */
  public void initGlobalIdAndMsgQueueMap() {
    globalIdAndMsgQueueMap.put(0, new LinkedBlockingQueue<Message>());
    for (int i : slaveArray) {
      globalIdAndMsgQueueMap.put(i, new LinkedBlockingQueue<Message>());
    }
  }

  @Override
  public void run() {
    System.out.println("The Master has started. Size: " + size);
    createThreads();
    sendRoundStartMsg();

    do {

      // if (globalIdAndMsgQueueMap.get(id).size() != slaveArray.length) {
      // System.out.println("Master waiting.");
      // continue;
      // }
      globalIdAndMsgQueueMap.get(id).drainTo(localMessageQueue);
      System.out
          .println("Master checking its queue. Size of queue is: " + localMessageQueue.size() + " round " + round);

      while (!(localMessageQueue.isEmpty())) {
        try {
          Message tempMsg = localMessageQueue.take();
          // System.out.println(tempMsg);

          if (tempMsg.getmType().equalsIgnoreCase("Leader")) {
            // if a node says it's Leader to master, master tells the node to terminate.
            localMessageQueue.clear();

            globalIdAndMsgQueueMap.get(tempMsg.getSenderId())
                .put(new Message(id, tempMsg.getSenderId(), 0, level, round, coreLink, "Terminate"));
            System.err.println("---Telling the master to die. Leader is: " + tempMsg.getSenderId() + " round " + round);
            masterMustDie = true;

            coreLink = tempMsg.getCore();
            killAll();
          } else if ((tempMsg.getmType().equals("Done"))) {
            processRoundDoneMsg(tempMsg);
            numberOfFinishedThreads++;
          }
          // all slaves completed the round
          if (numberOfFinishedThreads == slaveArray.length) {
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
      System.out.println("Starting threads. ");
      startAllThreads();
      sleep();
    } while (!masterMustDie);

    printTree();
    System.out.println("Master will now die. MaxUid " + coreLink + " round " + round);

  }

  /**
   * Print the nackAck set after the algorithm is done.
   */
  public void printTree() {
    System.out.println("\n\nPrinting the tree.");
    System.out.println("MaxId----Parent <--- myId ---> myChildren (can overlap)");
    for (SlaveThread t : threadList) {
      System.out.print(t.coreLink + "---  " + t.getMyParent() + "<------" + t.getId() + "------>");
      for (Link i : t.getBasicLinks()) {
        if (i.getFrom() != t.getMyParent()) {
          System.out.print(i + " ");
        }
      }
      System.out.println();
    }
  }

  public void processRoundDoneMsg(Message m) {
    if (m.isLeader()) {
      leaderSet.add(m.senderId);
    } else
      leaderSet.remove(m.getSenderId());
  }

  /**
   * Create all nodes/threads, set their names, neighbors, initiate the local copy
   * (two queues) of the global queue.
   */
  public void createThreads() {
    try {
      for (int row = 0; row < size; row++) {
        SlaveThread t = new SlaveThread(slaveArray[row], this, globalIdAndMsgQueueMap);
        leaderSet.add(slaveArray[row]);
        threadList.add(t);

        // add neighbors
        for (int col = 0; col < size; col++) {
          if (matrix[row][col] > 0) {
            // System.err.println(t.name + " " + slaveArray[row] + " " + slaveArray[col] + "
            // " + matrix[row][col]);
            t.insertNeighbour(new Link(slaveArray[row], slaveArray[col], matrix[row][col]));
          }
        }

        t.initLocalMessagesQueues();
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

  public synchronized void sendRoundStartMsg() {
    for (int i : slaveArray) {
      //System.err.println("Send Round_Number msg to " + i);
      try {
        Message temp = new Message(id, i, 0, level, round, coreLink, "Round_Number");
        globalIdAndMsgQueueMap.get(i).put(temp);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Debug only.
   */
  public void checkGlobalQueueNotEmpty() {
    for (Entry<Integer, LinkedBlockingQueue<Message>> p : globalIdAndMsgQueueMap.entrySet()) {
      System.out.println(p.getKey() + " " + p.getValue());
    }
  }

  /**
   * Set all slaves' terminate to true;
   */
  public void killAll() {
    for (SlaveThread s : threadList) {

      System.out.println("Terminated slave " + s.getId());
      try {
        s.terminated = true;

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
