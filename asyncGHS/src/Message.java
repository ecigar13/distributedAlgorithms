
import java.util.LinkedList;

public class Message {

  protected int senderId; // serial number in the graph
  protected int receiverId;
  protected double mwoe;
  protected Link core;
  protected int round;
  protected int level;
  protected LinkedList<Integer> path;
  protected String mType;
  protected int myParent;

  /**
   * Constructor for floodmax messages.
   * 
   * @param senderId
   * @param receiverId
   *          TODO
   * @param mwoe
   *          TODO
   * @param level
   *          TODO
   * @param round
   * @param core
   * @param mType
   */
  public Message(int senderId, int receiverId, double mwoe, int level, int round, Link core, String mType) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.mwoe = mwoe;
    this.core = core;
    this.level = level;
    this.round = round;
    this.mType = mType;
    this.path = new LinkedList<>();
  }

  public Link getCore() {
    return core;
  }

  public void setCore(Link core) {
    this.core = core;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public void setPath(LinkedList<Integer> path) {
    this.path = path;
  }

  public int getParent() {
    return myParent;
  }

  public void setParent(int myParent) {
    this.myParent = myParent;
  }

  public LinkedList<Integer> getPath() {
    return path;
  }

  public double getMwoe() {
    return mwoe;
  }

  public void setMwoe(double mwoe) {
    this.mwoe = mwoe;
  }

  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("mType ").append(mType).append(" Sender id: ").append(senderId).append(" core: ").append(core)
        .append(" level ").append(level);
    return stringBuffer.toString();
  }

  public int getReceiverId() {
    return receiverId;
  }

  public void setReceiverId(int receiverId) {
    this.receiverId = receiverId;
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getUid() {
    return senderId;
  }

  public void setUid(int uid) {
    this.senderId = uid;
  }

  public int getRound() {
    return round;
  }

  public void setRound(int round) {
    this.round = round;
  }

  public String getmType() {
    return mType;
  }

  public void setmType(String mType) {
    this.mType = mType;
  }

}
