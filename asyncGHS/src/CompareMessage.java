import java.util.Comparator;

public class CompareMessage implements Comparator<Message> {

  public CompareMessage() {

    // TODO Auto-generated constructor stub
  }

  @Override
  public int compare(Message o1, Message o2) {
    if (o1.getMwoe() > o2.getMwoe()) {
      return 1;
    } else if (o1.getMwoe() < o2.getMwoe()) {
      return -1;
    }
    return 0;
  }
}
