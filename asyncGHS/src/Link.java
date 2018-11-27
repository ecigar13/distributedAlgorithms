/**
 * 
 * @author khoa
 *
 */
public class Link {
  int from;
  int to;
  double weight;

  /**
   * 
   * @param from
   *          link from
   * @param to
   *          link to
   * @param weight
   *          edge weight
   */
  public Link(int from, int to, double weight) {
    this.to = to;
    this.from = from;
    this.weight = weight;
  }

  public int getFrom() {
    return from;
  }

  public void setFrom(int from) {
    this.from = from;
  }

  public int getTo() {
    return to;
  }

  public void setTo(int to) {
    this.to = to;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  public String toString() {
    return from + "-" + to + "-" + weight;
  }

}
