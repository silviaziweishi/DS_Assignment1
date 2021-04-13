import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.nio.ByteBuffer;
import java.util.Arrays;

class Record {
	
	int recordSize=0; //4byte
	int ID;  //4byte
	//mmddyyyy
	int Date; //4byte
	//tttt
	int Time; //4byte
	String Day; //10byte
	int SensorId;//4byte
	int Counts; //4byte
	String SensorName; //
	
}

class Page {
	
	ArrayList<Record> records = new ArrayList<Record>();
	
	public int totalBytesOfPage() {
		int totalBytes=0;
		for(Record rec : records) {
			totalBytes = totalBytes +rec.recordSize;
		}
		return totalBytes;
	}
	
	
	
}

public class dbload {
	
	public static String convertDateTimeToInt(String dateTime) {
		String[] temp = dateTime.split(" ");
		String[] date = temp[0].split("/");
		for(int i=0;i<2;i++) {
			if(date[i].length()<2) {
				date[i] = "0"+date[i];
			}
		}
		date[2] = "20"+date[2];
		String[] time = temp[1].split(":");
		return date[0]+date[1]+date[2]+" "+time[0]+time[1];
	}
	
	public static void writeheap(DataOutputStream os, Record record) throws IOException {
		
		os.write(ByteBuffer.allocate(4).putInt(record.recordSize).array());
		os.write(ByteBuffer.allocate(4).putInt(record.ID).array());
		os.write(ByteBuffer.allocate(4).putInt(record.Date).array());
		os.write(ByteBuffer.allocate(4).putInt(record.Time).array());
		os.write(Arrays.copyOf(record.Day.getBytes("UTF-8"),10));
		os.write(ByteBuffer.allocate(4).putInt(record.SensorId).array());
		os.write(ByteBuffer.allocate(4).putInt(record.Counts).array());
		os.write(record.SensorName.getBytes("UTF-8"));
	}
	
	public void heapFile(String[] args) throws IOException {
		int pageSize=0;
		File dataFile;
		int currentPage = 0;
		
		ArrayList<Page> pages = new ArrayList<Page>();
		
		try {
			pageSize = Integer.parseInt(args[args.length-2]);
		}catch(Exception e) {
			System.out.println("Page size must be an integer!");
		}
		
		dataFile = new File(args[args.length-1]);
		
		//temp[ID,DATE,TIME,DAY,SENSORID,COUNTS,SENSORNAME]
		try {
			Scanner reader = new Scanner(dataFile);
			DataOutputStream os = new DataOutputStream(new FileOutputStream("heap."+pageSize));
			reader.nextLine();
			while(reader.hasNextLine()) {
				Record record = new Record();
				//page.records.add(record);
				//pages.add(page);
				String[] rowData = reader.nextLine().split(",");
				record.ID = Integer.parseInt(rowData[0]);
				String dateTime =  convertDateTimeToInt(rowData[1]);
				record.Date = Integer.parseInt(dateTime.split(" ")[0]);
				record.Time = Integer.parseInt(dateTime.split(" ")[1]);
				record.Day = rowData[5];
				record.SensorId = Integer.parseInt(rowData[7]);
				record.SensorName = rowData[8];
				record.Counts = Integer.parseInt(rowData[9]);
				//ID,DATE,TIME,SENSORID,COUNTS are each 4 bytes and one addition of total bytes of each record which is 4 bytes. so 4*6
				// Day are max of 10 bytes.
				record.recordSize = 4*6+10+record.SensorName.getBytes().length;
				//when at the satrt, no pages in the arraylist, so create new instance.
				if(pages.size() == 0) {
					Page page1 = new Page();
					pages.add(page1);
				}
				//when the record size is smaller than the size of rest of the current page
				if(record.recordSize <= pageSize-pages.get(currentPage).totalBytesOfPage()) {
					pages.get(currentPage).records.add(record);
					writeheap(os, record);
				}
				//when the rest of the page is not enough for a new record, fill the rest with 0 and get a new page.
				else {
					for(int i=0;i<pageSize-pages.get(currentPage).totalBytesOfPage();i++) {
						os.write(0);
					}
					Page page1 = new Page();
					pages.add(page1);
					currentPage++;
					pages.get(currentPage).records.add(record);
					writeheap(os, record);
				}
			}
			/*for(int i=0;i<pageSize-pages.get(currentPage).totalBytesOfPage();i++) {
				os.write(0);
			}*/
			
		}catch(FileNotFoundException e) {
			System.out.println("Data File is not exist!");
		}
		
		System.out.println("The number of pages used: " + Integer.toString(pages.size()));
		int totalRecords = 0;
		for(Page pg: pages) {
			totalRecords = totalRecords + pg.records.size();
		}
		System.out.println("The number of records loaded: " + Integer.toString(totalRecords));
		
	}
	
	public static void main(String[] args) {
		//args = "java dbload -p 99 test1.csv".split(" ");
		
		dbload db = new dbload();
		long startTime = System.nanoTime();
		try {
			db.heapFile(args);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long endTime = System.nanoTime();
		long timetaken = (endTime-startTime)/1000000;
		System.out.println("The time taken to create heap file: " + String.valueOf(timetaken)+"ms");
	}
}
