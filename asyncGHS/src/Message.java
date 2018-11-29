
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
   * @param mwoe
   * @param level level of component.
   * @param round delay rounds. For round start msg, this is the current round.
   * @param core coreLink of component
   * @param mType message type. Used in message process factory.
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

  public Message setCore(Link core) {
    this.core = core;
    return this;
  }

  public int getLevel() {
    return level;
  }

  public Message setLevel(int level) {
    this.level = level;
    return this;
  }

  public Message setPath(LinkedList<Integer> path) {
    this.path = path;
    return this;
  }

  public int getParent() {
    return myParent;
  }

  public Message setParent(int myParent) {
    this.myParent = myParent;
    return this;
  }

  public LinkedList<Integer> getPath() {
    return path;
  }

  public double getMwoe() {
    return mwoe;
  }

  public Message setMwoe(double mwoe) {
    this.mwoe = mwoe;
    return this;
  }

  public String toString() {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("mType ").append(mType).append(" from ").append(senderId).append(" to ").append(receiverId)
        .append(" core ").append(core).append(" lvl ").append(level).append(path);
    return stringBuffer.toString();
  }

  public int getReceiverId() {
    return receiverId;
  }

  public Message setReceiverId(int receiverId) {
    this.receiverId = receiverId;
    return this;
  }

  public int getSenderId() {
    return senderId;
  }

  public Message setSenderId(int senderId) {
    this.senderId = senderId;
    return this;
  }

  public int getUid() {
    return senderId;
  }

  public Message setUid(int uid) {
    this.senderId = uid;
    return this;
  }

  public int getRound() {
    return round;
  }

  public Message setRound(int round) {
    this.round = round;
    return this;
  }

  public String getmType() {
    return mType;
  }

  public Message setmType(String mType) {
    this.mType = mType;
    return this;
  }

}
