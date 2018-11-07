import java.util.Comparator;
import java.util.TreeMap;

public class CompareLinks implements Comparator<Link> {

  public CompareLinks() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public int compare(Link o1, Link o2) {
    if (o1.getWeight() > o2.getWeight()) {
      return 1;
    } else if (o1.getWeight() < o2.getWeight()) {
      return -1;
    }
    return 0;
  }

}
