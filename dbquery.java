import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class dbquery {
	
	public byte[][] convertTo2dArray(byte[] bts, int pageSize) {
		int numOfPage;
		if (bts.length/pageSize==0) {
			numOfPage=bts.length/pageSize;
		}else {
			numOfPage=bts.length/pageSize+1;
		}
		
		byte[][] bts2d = new byte[numOfPage][pageSize];
		for(int i=0; i<numOfPage; i++) {
			for(int j=0; j<pageSize; j++) {
				if(i*pageSize+j<bts.length) {
					bts2d[i][j]=bts[i*pageSize+j];
				}else {
					bts2d[i][j]=0;
				}
			}
		}
		return bts2d;
	}
	//starPos is the position of starting point in array.(include)
	//len is the length to copy.
	public byte[] copyByteArray(byte[] bts, int starPos, int len) {
		byte[] copyOfArray = new byte[len];
		for(int i=0; i<len;i++) {
			copyOfArray[i] = bts[starPos+i];
		}
		return copyOfArray;
	}
	
	
	
	public void ifContainText(byte[] bts, String text) {
		
		
		String record = "";
		//ID
		record = record + Integer.toString(ByteBuffer.wrap(copyByteArray(bts, 4, 4)).getInt());
		record = record + ",";
		//date mmddyyyy 
		record = record + Integer.toString(ByteBuffer.wrap(copyByteArray(bts, 8, 4)).getInt());
		record = record + ",";
		// time 0000
		record = record + Integer.toString(ByteBuffer.wrap(copyByteArray(bts, 12, 4)).getInt());
		record = record + ",";
		//day
		record = record + new String(copyByteArray(bts, 16, 10));
		record = record + ",";
		//sensor ID
		record = record + Integer.toString(ByteBuffer.wrap(copyByteArray(bts, 26, 4)).getInt());
		record = record + ",";
		//counts
		record = record + Integer.toString(ByteBuffer.wrap(copyByteArray(bts, 30, 4)).getInt());
		record = record + ",";
		//sensorname
		record = record + new String(copyByteArray(bts, 34, bts.length-34));
		
		//System.out.println(record);
		if(record.contains(text)) {
			System.out.println(record);
		}
	}
	
	public void searchText(String[] args) throws IOException {
		String text=args[args.length-2];
		int pageSize=0;
		int totalByteLength;
		int currentPos=0;
		
		try {
			pageSize = Integer.parseInt(args[args.length-1]);
		}catch(Exception e) {
			System.out.println("Page size must be an integer!");
		}
		
		
		RandomAccessFile rda = new RandomAccessFile("heap."+pageSize,"r");
		
		
		Path path = Paths.get("heap."+pageSize);

		byte[]	bts = Files.readAllBytes(path);
		//each row is one page.
		byte[][] bts2d = convertTo2dArray(bts, pageSize);
		
		totalByteLength = bts.length;
		
		for(int i=0; i<bts2d.length;i++) {
			//int posRecordSize=0;
			//int recordSize=ByteBuffer.wrap(copyByteArray(bts2d[i],posRecordSize,4)).getInt();
			int recordSize=ByteBuffer.wrap(copyByteArray(bts2d[i],0,4)).getInt();
			
			for(int j=0; j<bts2d[0].length;j = j+recordSize) {
				if(ByteBuffer.wrap(copyByteArray(bts2d[i],j,4)).getInt()!=0) {
					recordSize=ByteBuffer.wrap(copyByteArray(bts2d[i],j,4)).getInt();
				}else {
					break;
				}
				ifContainText(copyByteArray(bts2d[i],j,recordSize),text);
				
			}
		}
		
	}
	
	public static void main(String[] args) {
		//args = "java dbload -p Queen 99".split(" ");
		
		dbquery db = new dbquery();
		long startTime = System.nanoTime();
		try {
			db.searchText(args);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long endTime = System.nanoTime();
		long timetaken = (endTime-startTime)/1000000;
		System.out.println();
		System.out.println("The time taken to search the text: " + String.valueOf(timetaken)+"ms");
	}
	
	
}
