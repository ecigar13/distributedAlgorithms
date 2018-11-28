
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;

import com.sun.swing.internal.plaf.basic.resources.basic;

import java.util.Collections;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;

public class SlaveThread implements Runnable {
  protected int delay = 1;

  protected String name;
  protected boolean terminated;

  protected int id;
  protected int round;
  private int myParent = -1;
  protected double mwoe;
  protected MasterThread masterNode;
  protected int level = 0;
  boolean mwoeFound = false;

  protected Link coreLink = null;
  protected boolean waitingToConnect = false;
  protected Message currentSmallestReportMessage;
  protected Message currentSmallestAcceptMsg;

  protected ConcurrentSkipListSet<Message> testMsgToRespond = new ConcurrentSkipListSet<>(new CompareMessage());
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToReduce;
  protected ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> localMsgToSend;
  protected LinkedBlockingQueue<Message> localMessageQueue = new LinkedBlockingQueue<>();

  protected HashSet<Integer> sentConnect = new HashSet<>();
  protected HashSet<Integer> receivedConnect = new HashSet<>();

  protected TreeSet<Integer> reportReceived = new TreeSet<>();
  protected HashSet<Integer> waitingForResponse = new HashSet<>();
  protected ConcurrentSkipListSet<Link> basicEdge = new ConcurrentSkipListSet<>(new CompareLinks());
  protected ConcurrentSkipListSet<Link> branch = new ConcurrentSkipListSet<>(new CompareLinks());
  protected ConcurrentSkipListSet<Link> rejected = new ConcurrentSkipListSet<>(new CompareLinks());

  protected Random r = new Random();

  /**
   * Merge when receiving connect msg. The node with higher ID becomes parent.
   * Move the Link to the right set. Increase level. Tell all its tree nodes that
   * it changed level and core.
   */
  public void merge(Message m) throws InterruptedException {
    System.out.printf("Merging %s receiving msg and %s\n", id, m.getSenderId());
    if (id < m.getSenderId()) {
      myParent = m.getReceiverId();
    } else {
      myParent = -1;

      for (Link l : basicEdge) {
        if (l.getTo() == m.getSenderId()) {
          coreLink = new Link(m.getReceiverId(), m.getSenderId(), l.getWeight());
        }
      }
    }

    // inefficient because using object.
    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        branch.add(l);

      }
    }

    level++;
    if (myParent == -1) {
      // broadcastchange component to the rest of the tree, change their core to this
      // core.
      broadcastLevelUp();
    }
  }

  /**
   * send levelUp to all branches. Call this when perform a merge.
   * 
   * @throws InterruptedException
   */
  public void broadcastLevelUp() throws InterruptedException {

    for (Link l : branch) {
      if (l.getTo() != myParent) {
        localMsgToReduce.get(l.getTo())
            .put(new Message(id, l.getTo(), mwoe, level, r.nextInt(delay) + 2, coreLink, "levelUp"));
      }
    }
  }

  public void processLevelUpMsg(Message m) throws InterruptedException {
    myParent = m.getSenderId();
    level = m.getLevel();
    coreLink = m.getCore();
    broadcastLevelUp();
  }

  /**
   * Process messages int the queue.
   */
  public void processMessageTypes() throws InterruptedException {
    Message m;
    while (localMessageQueue.size() != 0) {
      m = localMessageQueue.poll();
      System.out.println(name + " processing message " + m);

      if (m.getmType().equals("Round_Number")) {
        processRoundNumber(m);
      } else if (m.getmType().equals("initiate")) {
        processInitMessage(m);
      } else if (m.getmType().equals("test")) {
        processTestMsg(m);
      } else if (m.getmType().equals("report")) {
        processReportMessage(m);
      } else if (m.getmType().equals("accept")) {
        processAcceptMessage(m);
      } else if (m.getmType().equals("reject")) {
        processRejectMsg(m);
      } else if (m.getmType().equals("changeRoot")) {
        // changeRoot is essentially connect msg that hasn't reached the leaf yet.
        processConnectMessage(m);
      } else if (m.getmType().equals("connect")) {
        processConnectMessage(m);
      } else if (m.getmType().equals("levelMismatched")) {
        processLevelMismatchedMsg(m);
      } else if (m.getmType().equals("absorbed")) {
        childAbsorb(m);
      } else if (m.getmType().equals("levelUp")) {
        processLevelUpMsg(m);
      }
    }
  }

  /**
   * Local operation. Register child as absorbed. Send back "absorbed" message
   * with core and level.
   * 
   * @param m
   */
  public void parentAbsorb(Message m) throws InterruptedException {
    // clear all report because the core has changed.
    System.err.printf("%s parentAbsorb %s\n", name, m.getSenderId());
    reportReceived.clear();
    currentSmallestReportMessage = null;
    currentSmallestAcceptMsg = null;

    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        branch.add(l);
        break;
      }
    }

    System.err.printf("%s send absorb to %s", name, m.getSenderId());
    System.out.println("Parent Core link for absorb " + coreLink);
    localMsgToReduce.get(m.getSenderId())
        .put(new Message(id, m.getSenderId(), mwoe, level, r.nextInt(delay) + 2, coreLink, "absorbed"));
  }

  /**
   * Use with "absorbed" msg. The child combine into higher level component. Set
   * core, level, parent and broadcast to all branches.
   * 
   * @param m
   *          message from higher level component.
   */
  public void childAbsorb(Message m) throws InterruptedException {
    // clear all report because the core has changed.
    System.err.printf("%s childAbsorb %s\n", name, m.getSenderId());
    myParent = m.getSenderId();
    reportReceived.clear();
    currentSmallestReportMessage = null;
    currentSmallestAcceptMsg = null;

    level = m.getLevel();
    waitingToConnect = false;
    coreLink = m.getCore();

    for (Link l : basicEdge) {
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        branch.add(l);
        break;
      }
    }
    broadcastAbsorb(m);

  }

  /**
   * If I'm the mwoe node (outside the component), check if I sent connect msg to
   * the sender. Then merge or absorb base on the level. If absorb, send
   * "absorbed" msg back.
   * 
   * If I'm the leaf node (path size is 1), then this is the changeRoot message. I
   * record that I sent connect message. Change my level and core to fit. Then
   * send it.
   * 
   * Else just relay it. Save level and core.
   * 
   */
  public void processConnectMessage(Message m) throws InterruptedException, NullPointerException {
    if (m.getPath().size() == 0) {
      System.out.printf("%s processing connect msg from %s level %s\n", name, m.getSenderId(), m.getLevel());
      // if I sent connect msg before. Then merge or absorb.
      receivedConnect.add(m.getSenderId());

      if (sentConnect.contains(m.getSenderId()) && receivedConnect.contains(m.getSenderId()) && level == m.getLevel()) {
        merge(m); // merge on receiving side.
      } else if (receivedConnect.contains(m.getSenderId()) && level > m.getLevel()) {
        parentAbsorb(m);
      } // else, do nothing. Not enough condition to merge or absorb. See wikipedia.

    } else if (m.getPath().size() == 1) {
      System.out.printf("%s processing changeRoot msg from %s level %s\n", name, m.getSenderId(), m.getLevel());
      // mType should be changeRoot from parent

      level = m.getLevel();
      coreLink = m.getCore();

      int temp = m.getPath().removeLast();

      sentConnect.add(temp);

      m.setSenderId(id);
      m.setReceiverId(temp);
      m.setmType("connect"); // in case this is changeRoot msg.
      localMsgToReduce.get(temp).put(m);
      sentConnect.add(temp);
      waitingToConnect = true;

      for (Link l : basicEdge) {
        if (l.getTo() == m.getReceiverId()) {
          basicEdge.remove(l);
          branch.add(l);
        }
      }
    } else {

      level = m.getLevel();
      coreLink = m.getCore();

      // I'm in the same component, keep sending it.
      int temp = m.getPath().removeLast();
      m.setSenderId(id);
      m.setReceiverId(temp);

      // System.out.printf("DDDDDDD %s %s", name, temp);
      localMsgToReduce.get(temp).put(m);
    }

  }

  /**
   * Need to implement this. Don't use while loop.
   */
  public synchronized void run() {
    printSlave();
    if (!terminated) {
      try {
        sendTestToSmallestBasic();

        if (coreLink == null) {
        }
        fetchFromGlobalQueue();
        // if I'm leader, send initiate.
        processMessageTypes();
        processWaitingTestMessage();
        decideToSendReportMsg();
        // send msg I need to send into local queue.
        reduceRoundInMsg();
        sendRoundDoneToMaster();
        // printEdges();
        drainToGlobalQueue();
        printEdges();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    printSlave();
  }

  public synchronized void printEdges() {
    System.err.print(name);
    System.err.printf(" Print branch \n");
    for (Link l : branch) {
      System.err.println(l);
    }

    System.err.printf("Print basic \n");
    for (Link l : basicEdge) {
      System.err.println(l);
    }

    System.err.printf("Print reject \n");
    for (Link l : rejected) {
      System.err.println(l);
    }

  }

  /**
   * Only do this if I got all reports from all children and accept msg from
   * basic.
   * 
   * @throws InterruptedException
   */
  public void decideToSendReportMsg() throws InterruptedException {
    if (reportReceived.size() == branch.size() - 1 && currentSmallestAcceptMsg != null && myParent != -1) {

      // If I got report from all branches, Compare it with what I get from reports.
      Message msgToUse;
      if (currentSmallestReportMessage == null) {
        msgToUse = currentSmallestAcceptMsg;
      } else if (currentSmallestReportMessage.getMwoe() > currentSmallestAcceptMsg.getMwoe()) {
        msgToUse = currentSmallestAcceptMsg;
      } else {
        msgToUse = currentSmallestReportMessage;
      }

      // if I pick mwoe from basic edge, construct new msg and send up.
      // also add the last node.
      msgToUse.setmType("report");
      msgToUse.setSenderId(id);
      msgToUse.getPath().add(id);

      System.out.println("PPPPPPPPPP" + myParent);

      localMsgToReduce.get(myParent).put(msgToUse); // null pointer exception here.

    } else if (myParent == -1) {
      if (reportReceived.size() == branch.size() && currentSmallestAcceptMsg != null) {
        // if I'm the leader and I heard report and accept from everyone// If I got
        // report from all branches, Compare it with what I get from reports.
        Message tempMsg;

        if (currentSmallestReportMessage == null) {
          tempMsg = currentSmallestAcceptMsg;
        } else if (currentSmallestReportMessage.getMwoe() > currentSmallestAcceptMsg.getMwoe()) {
          tempMsg = currentSmallestAcceptMsg;
        } else {
          tempMsg = currentSmallestReportMessage;
        }

        if (tempMsg.getmType().equals("report")) {
          // if leader, process the message and broadcast back.
          sendChangeRootDown(tempMsg);

        } else if (tempMsg.getmType().equals("accept")) {
          // what if my mwoe is in leader's basic edges???
          // send connect to itself
          System.out.printf("%s sent connect to %s\n", name, tempMsg.getSenderId());
          Message temp = new Message(id, tempMsg.getSenderId(), mwoe, level, round, coreLink, "connect");
          sentConnect.add(tempMsg.getSenderId());

          localMsgToReduce.get(tempMsg.getSenderId()).put(temp);

        }
      }

      // clear my set of report and set mwoe to maximum.
      reportReceived.clear();
      currentSmallestReportMessage = null;
      currentSmallestAcceptMsg = null;
      mwoe = Double.MAX_VALUE;
    }
  }

  /**
   * Remove the sender from waiting for response (because I got the response).
   * Save the test msg.
   * 
   * @param m
   */
  public void processAcceptMessage(Message m) throws InterruptedException {
    // System.err.println("AAAAAAAAAAAAAAAAAAAAAAA");
    // System.out.println(m);
    // System.err.println(waitingForResponse);
    waitingForResponse.remove(m.getSenderId());
    currentSmallestAcceptMsg = m;

  }

  /**
   * Only run this method if leader got report msg from all its children.
   * 
   * If path is 1, then I'm the leaf node. Record that I sent connect to the mwoe
   * node and send it.
   * 
   * if currentReportMsg is not null then the mwoe belongs to my children. Send
   * the changeRoot message back down the path. This is changeRoot message. Near
   * the end of the path, this changeRoot becomes connect message.
   * 
   * @param m
   *          message to be used.
   * @param id
   * @throws InterruptedException
   */
  public void sendChangeRootDown(Message m) throws InterruptedException {
    // reuse the message.
    m.setLevel(level);
    m.setRound(r.nextInt(delay) + 2);
    m.setmType("changeRoot");

    // save behavior as processConnectMessage()
    processConnectMessage(m);

  }

  /**
   * Implement initial round, when coreLink is null.
   * 
   * Process Test messages. If my lvl is > your lvl. "accept" message will be used
   * to construct report msg, so always add the path. If my lvl < your lvl, maybe
   * I'm in the same component but I don't know yet.
   * 
   * @param m
   *          test msg to process
   */
  public void processTestMsg(Message m) throws InterruptedException {
    System.err.printf("%s processing test from %s\n", name, m.getSenderId());
    if (coreLink != null && m.getCore() != null && coreLink.getWeight() == m.getCore().getWeight()) {
      System.err.printf("%s sending reject to %s\n\n", name, m.getSenderId());
      localMsgToReduce.get(m.getSenderId())
          .put(new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(delay) + 2, coreLink, "reject"));

      return;
    }
    // dont send an accept message if waitingforResonse queue is not zero
    if (coreLink == null || coreLink != m.getCore()) {
      if (level >= m.getLevel()) {
        System.err.printf("%s sending accept to %d\n\n", name, m.getSenderId());

        Message temp = new Message(id, m.getSenderId(), m.getMwoe(), level, r.nextInt(delay) + 2, coreLink, "accept");
        temp.getPath().add(id); // important step in deciding to send report message.
        localMsgToReduce.get(m.getSenderId()).put(temp);

      } else {// can't decide, wait until level is high enough to respond. See wikipedia algo.
        testMsgToRespond.add(m); // not complete.

        // System.err.printf("%s sending levelMismatched to %s", name, m.getSenderId());
        // localMsgToReduce.get(m.getSenderId())
        // .put(new Message(id, m.getSenderId(), mwoe, level, r.nextInt(delay) + 2,
        // coreLink, "levelMismatched"));
      }
    }
  }

  /**
   * Respond to any msg that is at my level or below.
   * 
   */
  public void processWaitingTestMessage() throws InterruptedException {
    for (Message message : testMsgToRespond) {
      if (level >= message.getLevel()) {
        processTestMsg(message);
      }
    }
  }

  /**
   * Send initiate to all branches except parent. Trigger find mwoe.
   */
  public void sendInitiateToBranch() throws InterruptedException {
    for (Link e : branch) {
      if (e.getFrom() == myParent) {
        continue;
      }
      if (!waitingForResponse.contains(e.getTo())) {
        System.out.println("Send initiate to " + e.getTo());
        localMsgToReduce.get(e.getTo())
            .put(new Message(id, e.getTo(), e.getWeight(), level, r.nextInt(delay) + 2, coreLink, "initiate"));
        waitingForResponse.add(e.getTo());
      }
    }
  }

  /**
   * Send test message to the smallest edge in basic edges. Don't wait for
   * response because bookkeeping for test msg is too much.
   * 
   * @throws InterruptedException
   */
  public void sendTestToSmallestBasic() throws InterruptedException {
    System.err.println(name + " wwww " + waitingForResponse);
    System.err.println(basicEdge);
    if (basicEdge.size() > 0) {
      Link e = basicEdge.first();
      if (!waitingForResponse.contains(e.getTo())) {
        System.out.println(name + " send test to " + e.getTo());
        localMsgToReduce.get(e.getTo())
            .put(new Message(id, e.getTo(), e.getWeight(), level, r.nextInt(delay) + 2, coreLink, "test"));
        waitingForResponse.add(e.getTo());

      } else {
        System.out.printf("%s waiting for response from %s\n", name, e.getTo());
      }
    }
  }

  /**
   * Record the msg as received. Save the report msg if it has smaller mwoe than
   * previious report msg. Otherwise, discard.
   * 
   * @param m
   */
  public void processReportMessage(Message m) throws InterruptedException {
    // save the msg with smallest mwoe. Discard the rest.
    reportReceived.add(m.getSenderId());
    if (currentSmallestReportMessage == null || currentSmallestReportMessage.getMwoe() > m.getMwoe()) {
      currentSmallestReportMessage = m;
    }
  }

  /**
   * Print out the hashmap/queue to debug.
   * 
   * @param hm
   */
  public synchronized void printMessagesToSendMap(ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> hm) {
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : hm.entrySet()) {
      System.err.println(e.getValue().size() + " " + e.getKey() + " " + e.getValue());
    }
    // sleep();
  }

  public void insertNeighbour(Link l) {
    this.basicEdge.add(l);
  }

  public int getId() {
    return id;
  }

  public void sleep() {
    try {
      Thread.sleep(0);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getMyParent() {
    return myParent;
  }

  public ConcurrentSkipListSet<Link> getBasicLinks() {
    return basicEdge;
  }

  /**
   * Go through messages in my queue and reduce the round by 1. Not less than 0.
   * Move all msg that are 0 at the front to the localMsgToSend queue.
   * 
   * Maybe implement map reduce.
   */
  public void reduceRoundInMsg() throws InterruptedException {
    System.out.printf("%s reducing msg round.\n", name);
    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToReduce.entrySet()) {
      for (Message m : e.getValue()) {
        int temp = m.getRound() - 1 > 0 ? m.getRound() - 1 : 0;
        m.setRound(temp);
      }

      while (e.getValue().size() > 0 && e.getValue().peek().getRound() <= 0) {
        localMsgToSend.get(e.getKey()).put(e.getValue().remove());
      }

    }
  }

  /**
   * Drain the local msg queue and put all in the global queue. Call this at the
   * end of each round. The global queue's structure is similar to this.
   */
  public synchronized void drainToGlobalQueue() {

    for (Entry<Integer, LinkedBlockingQueue<Message>> e : localMsgToSend.entrySet()) {
      e.getValue().drainTo(globalIdAndMsgQueueMap.get(e.getKey()));

      if (!e.getValue().isEmpty()) {
        System.err.println("Queue is not empty at end of round");
      }

    }
  }

  public ConcurrentSkipListSet<Link> getBranch() {
    return branch;
  }

  public void setBranch(ConcurrentSkipListSet<Link> branch) {
    this.branch = branch;
  }

  /**
   * Initiate the local queues. Otherwise it will throw nullPointerException.
   */
  public void initLocalMessagesQueues() {
    localMsgToReduce.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToReduce.put(l.getTo(), new LinkedBlockingQueue<Message>());
    }

    localMsgToSend.put(masterNode.getId(), new LinkedBlockingQueue<Message>());
    for (Link l : basicEdge) {
      localMsgToSend.put(l.getTo(), new LinkedBlockingQueue<Message>());
    }
    // System.err.println("Done initiating SlaveThread queues.");
  }

  /**
   * Put round done message to local queue.
   */
  public void sendRoundDoneToMaster() throws InterruptedException {
    Message temp = new Message(id, masterNode.getId(), mwoe, level, round, coreLink, "Done");
    System.err.println(name + " basic edge " + basicEdge);
    if (basicEdge.size() != 0) {
      temp.setParent(-1);
    } else {
      temp.setParent(myParent);
    }
    localMsgToSend.get(masterNode.getId()).put(temp);

  }

  /**
   * Drain the global queue to this local queue. The Global queue will have zero
   * element.
   * 
   * @return
   */
  public void fetchFromGlobalQueue() {
    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      globalIdAndMsgQueueMap.get(id).drainTo(localMessageQueue);
    }

    if (!globalIdAndMsgQueueMap.get(id).isEmpty()) {
      System.err.println("Global queue is not empty. We have a prolem.");
    }
  }

  /**
   * set my round to the round master tells me. Will not confuse with delayRound
   * in message.
   * 
   * @param m
   */
  public void processRoundNumber(Message m) {
    round = m.getRound();
  }

  /**
   * Send initiate to all branches and send test to smallest basic edge.
   * 
   * @param m
   */
  public void processInitMessage(Message m) throws InterruptedException {
    sendTestToSmallestBasic();
    sendInitiateToBranch();
  }

  /**
   * Add the edge to rejected set. Remove from basic edge.Send test msg to the
   * smallest basic edge.
   * 
   * @param m
   */
  public void processRejectMsg(Message m) throws InterruptedException {
    waitingForResponse.remove(m.getSenderId());
    for (Link l : basicEdge) {
      System.err.printf("Link %s message %s", l, m);
      if (l.getTo() == m.getSenderId()) {
        basicEdge.remove(l);
        rejected.add(l);

      }
    }
    sendTestToSmallestBasic();
  }

  /**
   * If level is mismatched, send test to the next one.
   * 
   * @param m
   * @throws InterruptedException
   */
  public void processLevelMismatchedMsg(Message m) throws InterruptedException {
    waitingForResponse.remove(m.getSenderId());
    ArrayList<Link> a = new ArrayList<>();
    while (basicEdge.size() != 0 && basicEdge.first().getTo() != m.getSenderId()) {
      a.add(basicEdge.pollFirst());
    }
    a.add(basicEdge.pollFirst());
    // send test to the next smallest edge.

    if (basicEdge.size() != 0) {
      Link temp = basicEdge.first();
      if (!waitingForResponse.contains(temp.getTo())) {
        System.out.println(name + " send test to " + temp.getTo());
        localMsgToReduce.get(temp.getTo())
            .put(new Message(id, temp.getTo(), temp.getWeight(), level, r.nextInt(delay) + 2, coreLink, "test"));
        waitingForResponse.add(temp.getTo());

      } else {
        System.out.printf("%s waiting for response from %s\n", name, temp.getTo());
      }

      for (Link l : a) {
        basicEdge.add(l);
      }
    }
  }

  /**
   * Broadcast absorb to all branches except parents.
   * 
   * @param m
   *          "absorbed" message received.
   * @throws InterruptedException
   */
  public void broadcastAbsorb(Message m) throws InterruptedException {
    if (branch.isEmpty()) {
      return;
    }

    for (Link l : branch) {
      if (l.getTo() == myParent) {
        continue;
      }
      // broadcast absorbed msg to all branches.
      localMsgToReduce.get(l.getTo())
          .put(new Message(id, l.getTo(), mwoe, level, r.nextInt(delay) + 2, coreLink, "absorbed"));
    }
  }

  public ConcurrentSkipListSet<Link> getRejected() {
    return rejected;
  }

  public SlaveThread() {
  }

  /**
   * Constructor.
   * 
   * @param id
   * @param masterNode
   */

  public SlaveThread(int id, MasterThread masterNode,
      ConcurrentHashMap<Integer, LinkedBlockingQueue<Message>> globalIdAndMsgQueueMap) {
    this.id = id;
    this.coreLink = null;
    this.myParent = -1;
    this.mwoe = Double.MAX_VALUE;
    this.masterNode = masterNode;
    this.round = 0;
    this.terminated = false;

    name = "Thread_" + id;

    this.globalIdAndMsgQueueMap = globalIdAndMsgQueueMap;
    this.localMsgToReduce = new ConcurrentHashMap<>();
    this.localMsgToSend = new ConcurrentHashMap<>();
    // init local messages to send will be done after construction in MasterThread
    // because Links are added after construction.
  }

  public void printSlave() {
    System.out.printf("%s printing myParent %s level %s coreLink %s\n testMsgToRespond %s\n\n", name, myParent, level,
        coreLink, testMsgToRespond.size());
    for (Message m : testMsgToRespond) {
      System.err.println(m + "DDDDDDDD");
    }
  }

}