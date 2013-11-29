public class RevTime{
	private long rev;
	private long ts;
	
	public RevTime(){
		this.rev = 0;
		this.ts = 0;
	}
	
	public RevTime(long rev, long ts){
		this.rev = rev;
		this.ts = ts;
	}
	
	public long getRev(){
		return this.rev;
	}
	
	public long getTs(){
		return this.ts;
	}
	
	public String toString(){
		return rev + " " + ts;
	}
}
