public class KeyValue{
	private long key;
	private int value;
	
	public KeyValue(){
		this.key=0;
		this.value=0;
	}
	
	public KeyValue(long key, int value){
		this.key = key;
		this.value = value;
	}
	
	public long getKey(){
		return this.key;
	}
	
	public int getValue(){
		return this.value;
	}
}
