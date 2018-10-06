package message;

public class Message {

  public enum MessageType {
    NACK, REJECT, IAMLEADER, EXPLORE, NOTLEADER, ROUNDDONE, DEBUG, DIAMETER;
  }

  protected int from;
  protected int to;

  public int getTo() {
    return to;
  }

  public void setTo(int to) {
    this.to = to;
  }

  protected MessageType mType;
  protected int senderId;
  protected int messageUid;
  protected int round;
  protected int diam;

  public int getDiam() {
    return diam;
  }

  public void setDiam(int diam) {
    this.diam = diam;
  }

  /**
   * Constructor for finding diameter messages.
   * 
   * @param senderId
   * @param from
   * @param to
   * @param distanceFromTo
   * @param mType
   */
  public Message(int senderId, int from, int to, int distanceFromTo, MessageType mType) {
    this.senderId = senderId;
    this.mType = mType;
    this.from = from;
    this.to = to;
    this.diam = distanceFromTo;
  }

  public int getTarget() {
    return to;
  }

  public void setTarget(int target) {
    this.to = target;
  }

  public int getFrom() {
    return from;
  }

  public void setFrom(int initiator) {
    this.from = initiator;
  }

  /**
   * Constructor for floodmax messages.
   * 
   * @param senderId
   * @param maxUid
   * @param mType
   * @param round
   */
  public Message(int senderId, int maxUid, MessageType mType, int round) {
    this.senderId = senderId;
    this.messageUid = maxUid;
    this.mType = mType;
    this.round = round;
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getMessageUid() {
    return messageUid;
  }

  public void setMessageUid(int messageUid) {
    this.messageUid = messageUid;
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

  public MessageType getmType() {
    return mType;
  }

  public void setmType(MessageType mType) {
    this.mType = mType;
  }

}
