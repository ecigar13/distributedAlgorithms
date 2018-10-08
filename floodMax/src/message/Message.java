package message;

public class Message {

  public enum MessageType {
    NACK, REJECT, IAMLEADER, EXPLORE, NOTLEADER, ROUNDDONE, DEBUG, DIAMETER;
  }

  protected int from;
  protected int to;

  protected MessageType mType;
  protected int senderId;
  protected int messageUid;
  protected int round;
  protected int distanceFromTo;

  /**
   * Constructor for finding diameter messages.
   * 
   * @param senderId
   * @param from
   * @param to
   * @param distanceFromTo
   * @param mType
   */
  public Message(int senderId, int from, int distanceFromTo, MessageType mType) {
    this.senderId = senderId;
    this.mType = mType;
    this.from = from;
    this.distanceFromTo = distanceFromTo;
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

  public int getTo() {
    return to;
  }

  public void setTo(int to) {
    this.to = to;
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

  public int getDistanceFromTo() {
    return distanceFromTo;
  }

  public void setDistanceFromTo(int distanceFromTo) {
    this.distanceFromTo = distanceFromTo;
  }
}
