package common.messages;

public class TreeElement {
	public IVectorClock clock;
	public String value;
	public TreeElement(IVectorClock clock, String value) {
		this.clock = clock;
		this.value = value;
	}
}
